// seaweedperf: compare SeaweedFS performance through the S3 gateway (AWS SDK)
// and directly against the filer HTTP API, with repeatable, comparable timings.
//
// You choose what to test with -backend (s3, filer, or s3,filer). The whole
// benchmark is repeated -runs times and results are reported as mean±stddev.
//
// First time only (fetches the AWS SDK and writes go.sum):
//
//	go mod tidy && go build -o seaweedperf .
//
// Examples:
//
//	./seaweedperf -backend s3
//	./seaweedperf -backend s3,filer -runs 5
//	./seaweedperf -backend s3 -label fsync=off -out base.json
//	./seaweedperf -backend s3 -label fsync=on  -baseline base.json
//	./seaweedperf -backend s3,filer -purge        # remove leftovers after a hard kill
//
// Everything is written under seaweedperf/<run-id>/ and removed on exit, including
// after errors and Ctrl-C. Both backends write under the same filer directory
// (/buckets/<bucket>), so path-specific filer settings apply to both.
//
// Exit status: 0 ok, 1 errors or regression, 2 bad usage / baseline not comparable,
// 130 interrupted.
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// All benchmark data lives under this key prefix, so it is easy to find and purge.
const rootPrefix = "seaweedperf/"

var phases = []string{"write", "read", "delete"}

// ---------------------------------------------------------------- backends --

// backend is the whole surface a storage path has to implement. Adding another
// path (e.g. the volume server directly) means adding one more of these.
type backend interface {
	Setup(ctx context.Context) error
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	// RemovePrefix deletes everything whose key starts with prefix (cleanup).
	RemovePrefix(ctx context.Context, prefix string) error
}

// s3Backend talks to the S3 gateway using the AWS SDK for Go v2.
type s3Backend struct {
	client *s3.Client
	bucket string
}

func newS3Backend(endpoint, bucket, access, secret, region string, hc *http.Client) *s3Backend {
	var creds aws.CredentialsProvider = aws.AnonymousCredentials{}
	if access != "" {
		creds = credentials.NewStaticCredentialsProvider(access, secret, "")
	}
	cfg := aws.Config{Region: region, Credentials: creds, HTTPClient: hc}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// Newer SDKs add checksum trailers by default, which many S3-compatible
		// servers don't expect. Only send them when the API requires it.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &s3Backend{client: client, bucket: bucket}
}

func (b *s3Backend) Setup(ctx context.Context) error {
	if _, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &b.bucket}); err == nil {
		return nil // already exists
	}
	_, err := b.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &b.bucket})
	return err
}

func (b *s3Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &b.bucket, Key: &key, Body: bytes.NewReader(data),
	})
	return err
}

func (b *s3Backend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (b *s3Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &key})
	return err
}

// RemovePrefix lists every object under prefix and deletes them in batches.
func (b *s3Backend) RemovePrefix(ctx context.Context, prefix string) error {
	pages := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{Bucket: &b.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}
		ids := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, o := range page.Contents {
			ids = append(ids, types.ObjectIdentifier{Key: o.Key})
		}
		out, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &b.bucket,
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf("%d deletes failed (first: %s)", len(out.Errors), aws.ToString(out.Errors[0].Key))
		}
	}
	return nil
}

// filerBackend talks to the filer HTTP API directly: PUT/GET/DELETE <dir>/<key>.
// Parent directories are created by the filer on write.
type filerBackend struct {
	base, dir string
	hc        *http.Client
}

func (b *filerBackend) url(key string) string { return b.base + b.dir + "/" + key }

func (b *filerBackend) Setup(ctx context.Context) error { return nil }

func (b *filerBackend) Put(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", b.url(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return b.do(req, "PUT "+key, 200, 201)
}

func (b *filerBackend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", b.url(key), nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: HTTP %d", key, resp.StatusCode)
	}
	return resp.Body, nil
}

func (b *filerBackend) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", b.url(key), nil)
	if err != nil {
		return err
	}
	return b.do(req, "DELETE "+key, 200, 202, 204)
}

// RemovePrefix deletes a directory tree. 404 means there was nothing to remove.
func (b *filerBackend) RemovePrefix(ctx context.Context, prefix string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", b.url(prefix)+"?recursive=true", nil)
	if err != nil {
		return err
	}
	return b.do(req, "DELETE "+prefix, 200, 202, 204, 404)
}

// do sends req, drains the body, and checks the status is one of ok.
func (b *filerBackend) do(req *http.Request, what string, ok ...int) error {
	resp, err := b.hc.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	for _, c := range ok {
		if resp.StatusCode == c {
			return nil
		}
	}
	return fmt.Errorf("%s: HTTP %d", what, resp.StatusCode)
}

// readObject fetches key and checks it matches want (by size, and by SHA-256
// when verify is set). Returns bytes read.
func readObject(ctx context.Context, be backend, key string, want []byte, verify bool) (int64, error) {
	body, err := be.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	h := sha256.New()
	var dst io.Writer = io.Discard
	if verify {
		dst = h
	}
	n, err := io.Copy(dst, body)
	if err != nil {
		return n, err
	}
	if n != int64(len(want)) {
		return n, fmt.Errorf("GET %s: got %d bytes, want %d", key, n, len(want))
	}
	if verify {
		w := sha256.Sum256(want)
		if !bytes.Equal(h.Sum(nil), w[:]) {
			return n, fmt.Errorf("GET %s: content mismatch", key)
		}
	}
	return n, nil
}

// ----------------------------------------------------------------- results --

// result is one phase of one run.
type result struct {
	Backend   string  `json:"backend"`
	Phase     string  `json:"phase"`
	Size      int64   `json:"size_bytes"`
	Ops       int     `json:"ops"`    // successful operations
	Errors    int     `json:"errors"` // failed or timed-out operations
	Seconds   float64 `json:"seconds"`
	OpsPerSec float64 `json:"ops_per_sec"` // successful ops only
	MBPerSec  float64 `json:"mb_per_sec"`
	P50ms     float64 `json:"p50_ms"` // latency percentiles cover ALL attempts,
	P95ms     float64 `json:"p95_ms"` // including failures and timeouts
	P99ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
}

type stat struct {
	Mean float64 `json:"mean"`
	SD   float64 `json:"sd"` // sample standard deviation across runs (0 if one run)
}

// summary aggregates one backend/phase/size across all runs.
type summary struct {
	Backend   string `json:"backend"`
	Phase     string `json:"phase"`
	Size      int64  `json:"size_bytes"`
	Runs      int    `json:"runs"`
	Errors    int    `json:"errors"` // total across runs
	OpsPerSec stat   `json:"ops_per_sec"`
	MBPerSec  stat   `json:"mb_per_sec"`
	P50ms     stat   `json:"p50_ms"`
	P95ms     stat   `json:"p95_ms"`
	P99ms     stat   `json:"p99_ms"`
	MaxMs     stat   `json:"max_ms"`
}

type report struct {
	Time          string     `json:"time"`
	RunID         string     `json:"run_id"`
	Label         string     `json:"label"`
	Backends      []string   `json:"backends"`
	S3Endpoint    string     `json:"s3_endpoint"`
	FilerEndpoint string     `json:"filer_endpoint"`
	Bucket        string     `json:"bucket"`
	Concurrency   int        `json:"concurrency"`
	Count         int        `json:"count_per_size"`
	Runs          int        `json:"runs"`
	Verify        bool       `json:"verify"`
	Seed          int64      `json:"seed"`
	TimeoutSec    float64    `json:"op_timeout_sec"`
	PerRun        [][]result `json:"per_run"`
	Summary       []summary  `json:"summary"`
}

func meanSD(xs []float64) stat {
	n := float64(len(xs))
	if n == 0 {
		return stat{}
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / n
	if n < 2 {
		return stat{Mean: mean}
	}
	var ss float64
	for _, x := range xs {
		ss += (x - mean) * (x - mean)
	}
	return stat{Mean: mean, SD: math.Sqrt(ss / (n - 1))}
}

// summarize groups per-run results by backend/phase/size, in a stable order.
func summarize(names []string, sizes []int64, perRun [][]result) []summary {
	groups := map[string][]result{}
	for _, run := range perRun {
		for _, r := range run {
			k := fmt.Sprintf("%s|%s|%d", r.Backend, r.Phase, r.Size)
			groups[k] = append(groups[k], r)
		}
	}
	var out []summary
	for _, name := range names {
		for _, size := range sizes {
			for _, phase := range phases {
				rs := groups[fmt.Sprintf("%s|%s|%d", name, phase, size)]
				if len(rs) == 0 {
					continue
				}
				col := func(f func(result) float64) stat {
					xs := make([]float64, len(rs))
					for i, r := range rs {
						xs[i] = f(r)
					}
					return meanSD(xs)
				}
				s := summary{
					Backend: name, Phase: phase, Size: size, Runs: len(rs),
					OpsPerSec: col(func(r result) float64 { return r.OpsPerSec }),
					MBPerSec:  col(func(r result) float64 { return r.MBPerSec }),
					P50ms:     col(func(r result) float64 { return r.P50ms }),
					P95ms:     col(func(r result) float64 { return r.P95ms }),
					P99ms:     col(func(r result) float64 { return r.P99ms }),
					MaxMs:     col(func(r result) float64 { return r.MaxMs }),
				}
				for _, r := range rs {
					s.Errors += r.Errors
				}
				out = append(out, s)
			}
		}
	}
	return out
}

// ---------------------------------------------------------------- benchmark -

type config struct {
	count, conc, warmup int
	timeout             time.Duration
	seed                int64
	verify              bool
}

// runPhase calls op(i) for i in [0,n) across c.conc workers. Every attempt is
// timed, including ones that fail or hit the per-op timeout, so slow failures
// show up in the percentiles and max. op returns the bytes it moved.
func runPhase(ctx context.Context, c config, name string, size int64, n int, op func(ctx context.Context, i int) (int64, error)) result {
	jobs := make(chan int)
	lats := make([][]time.Duration, c.conc) // lats[w] is only touched by worker w
	var okOps, errCount, bytesMoved int64
	var firstErrs []string
	var mu sync.Mutex

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < c.conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range jobs {
				t0 := time.Now()
				opCtx, cancel := context.WithTimeout(ctx, c.timeout)
				nb, err := op(opCtx, i)
				cancel()
				lats[w] = append(lats[w], time.Since(t0))

				mu.Lock()
				if err != nil {
					errCount++
					if len(firstErrs) < 3 {
						firstErrs = append(firstErrs, err.Error())
					}
				} else {
					okOps++
					bytesMoved += nb
				}
				mu.Unlock()
			}
		}(w)
	}
dispatch:
	for i := 0; i < n; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		idx := int(math.Ceil(p*float64(len(all)))) - 1
		if idx < 0 {
			idx = 0
		}
		return float64(all[idx].Microseconds()) / 1000
	}
	r := result{
		Phase: name, Size: size, Ops: int(okOps), Errors: int(errCount), Seconds: elapsed,
		OpsPerSec: float64(okOps) / elapsed,
		MBPerSec:  float64(bytesMoved) / 1e6 / elapsed,
		P50ms:     pct(0.50), P95ms: pct(0.95), P99ms: pct(0.99),
	}
	if len(all) > 0 {
		r.MaxMs = float64(all[len(all)-1].Microseconds()) / 1000
	}
	if ctx.Err() == nil { // after an interrupt the errors are just cancellations
		for _, e := range firstErrs {
			fmt.Fprintf(os.Stderr, "  [%s %s] error: %s\n", name, human(size), e)
		}
	}
	return r
}

// benchBackend runs warmup + write/read/delete for one backend and size.
func benchBackend(ctx context.Context, c config, name string, be backend, size int64, prefix string) []result {
	// Deterministic payload: same seed and size produce the same bytes.
	data := make([]byte, size)
	rand.New(rand.NewSource(c.seed + size)).Read(data)
	key := func(kind string, i int) string {
		return fmt.Sprintf("%s%s-%d/%06d", prefix, kind, size, i)
	}
	put := func(kind string) func(context.Context, int) (int64, error) {
		return func(ctx context.Context, i int) (int64, error) {
			if err := be.Put(ctx, key(kind, i), data); err != nil {
				return 0, err
			}
			return size, nil
		}
	}
	del := func(kind string) func(context.Context, int) (int64, error) {
		return func(ctx context.Context, i int) (int64, error) { return 0, be.Delete(ctx, key(kind, i)) }
	}
	read := func(ctx context.Context, i int) (int64, error) {
		return readObject(ctx, be, key("obj", i), data, c.verify)
	}

	// Warmup (untimed): open connections and touch the write path.
	if c.warmup > 0 {
		runPhase(ctx, c, "warmup", size, c.warmup, put("warm"))
		runPhase(ctx, c, "warmup", size, c.warmup, del("warm"))
	}

	fmt.Fprintf(os.Stderr, "  %s %s: write...\n", name, human(size))
	w := runPhase(ctx, c, "write", size, c.count, put("obj"))
	fmt.Fprintf(os.Stderr, "  %s %s: read...\n", name, human(size))
	r := runPhase(ctx, c, "read", size, c.count, read)
	fmt.Fprintf(os.Stderr, "  %s %s: delete...\n", name, human(size))
	d := runPhase(ctx, c, "delete", size, c.count, del("obj"))

	out := []result{w, r, d}
	for i := range out {
		out[i].Backend = name
	}
	return out
}

// ---------------------------------------------------------------- reporting -

// pm formats a stat as mean±sd (or just the mean when there was a single run).
func pm(s stat, runs int) string {
	if runs < 2 {
		return fmt.Sprintf("%.1f", s.Mean)
	}
	return fmt.Sprintf("%.1f±%.1f", s.Mean, s.SD)
}

func printTable(rep report) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BACKEND\tPHASE\tSIZE\tERR\tOPS/S\tMB/S\tP50ms\tP95ms\tP99ms\tMAXms")
	for _, s := range rep.Summary {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			s.Backend, s.Phase, human(s.Size), s.Errors,
			pm(s.OpsPerSec, s.Runs), pm(s.MBPerSec, s.Runs), pm(s.P50ms, s.Runs),
			pm(s.P95ms, s.Runs), pm(s.P99ms, s.Runs), pm(s.MaxMs, s.Runs))
	}
	tw.Flush()
}

func pctChange(from, to float64) float64 {
	if from == 0 {
		return 0
	}
	return (to - from) / from * 100
}

// compareBackends prints S3 relative to the filer (using means) so the cost of
// the gateway is easy to read. Only used when both backends ran.
func compareBackends(sums []summary) {
	filer := map[string]summary{}
	for _, s := range sums {
		if s.Backend == "filer" {
			filer[fmt.Sprintf("%s|%d", s.Phase, s.Size)] = s
		}
	}
	fmt.Println("\nS3 gateway vs direct filer, using means (latency + = S3 slower; ops/s - = S3 slower)")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PHASE\tSIZE\tP50 filer\tP50 s3\tΔP50\tP95 filer\tP95 s3\tΔP95\tOPS/S filer\tOPS/S s3\tΔOPS/S")
	for _, s := range sums {
		f, ok := filer[fmt.Sprintf("%s|%d", s.Phase, s.Size)]
		if s.Backend != "s3" || !ok {
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%.1f\t%.1f\t%+.0f%%\t%.1f\t%.1f\t%+.0f%%\t%.1f\t%.1f\t%+.0f%%\n",
			s.Phase, human(s.Size),
			f.P50ms.Mean, s.P50ms.Mean, pctChange(f.P50ms.Mean, s.P50ms.Mean),
			f.P95ms.Mean, s.P95ms.Mean, pctChange(f.P95ms.Mean, s.P95ms.Mean),
			f.OpsPerSec.Mean, s.OpsPerSec.Mean, pctChange(f.OpsPerSec.Mean, s.OpsPerSec.Mean))
	}
	tw.Flush()
}

// significant reports whether the difference between two stats is bigger than
// run-to-run noise (2 combined standard deviations). With fewer than two runs
// on either side there is no noise estimate, so the threshold alone decides.
func significant(base, cur stat, baseRuns, curRuns int) bool {
	if baseRuns < 2 || curRuns < 2 {
		return true
	}
	return math.Abs(cur.Mean-base.Mean) > 2*math.Hypot(base.SD, cur.SD)
}

// checkComparable returns an error if the two runs used different settings, since
// comparing them would blame a config change on the server.
func checkComparable(base, cur report) error {
	var diffs []string
	if base.Concurrency != cur.Concurrency {
		diffs = append(diffs, fmt.Sprintf("concurrency %d vs %d", base.Concurrency, cur.Concurrency))
	}
	if base.Count != cur.Count {
		diffs = append(diffs, fmt.Sprintf("count %d vs %d", base.Count, cur.Count))
	}
	if base.Verify != cur.Verify {
		diffs = append(diffs, fmt.Sprintf("verify %v vs %v", base.Verify, cur.Verify))
	}
	if len(diffs) > 0 {
		return fmt.Errorf("baseline is not comparable (baseline vs now): %s", strings.Join(diffs, ", "))
	}
	return nil
}

// compareBaseline prints deltas vs a saved run and returns true if any
// regression exceeds thr percent (p95 up, ops/s down) and is beyond run-to-run
// noise, or if errors appeared that the baseline did not have.
func compareBaseline(base, cur report, thr float64) (bool, error) {
	if err := checkComparable(base, cur); err != nil {
		return false, err
	}
	idx := map[string]summary{}
	for _, s := range base.Summary {
		idx[fmt.Sprintf("%s|%s|%d", s.Backend, s.Phase, s.Size)] = s
	}
	fmt.Printf("\nComparison vs baseline (label %q, %s, %d run(s)), threshold %.0f%%\n", base.Label, base.Time, base.Runs, thr)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BACKEND\tPHASE\tSIZE\tP95 base\tP95 now\tΔP95\tOPS/S base\tOPS/S now\tΔOPS/S\t")
	regressed, matched := false, 0
	for _, s := range cur.Summary {
		b, ok := idx[fmt.Sprintf("%s|%s|%d", s.Backend, s.Phase, s.Size)]
		if !ok || b.P95ms.Mean == 0 || b.OpsPerSec.Mean == 0 {
			continue
		}
		matched++
		dp := pctChange(b.P95ms.Mean, s.P95ms.Mean)
		do := pctChange(b.OpsPerSec.Mean, s.OpsPerSec.Mean)
		slow := dp > thr && significant(b.P95ms, s.P95ms, b.Runs, s.Runs)
		drop := do < -thr && significant(b.OpsPerSec, s.OpsPerSec, b.Runs, s.Runs)
		verdict := ""
		switch {
		case s.Errors > 0 && b.Errors == 0:
			verdict, regressed = "NEW ERRORS", true
		case slow || drop:
			verdict, regressed = "REGRESSION", true
		case dp > thr || do < -thr:
			verdict = "within noise"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.1f\t%.1f\t%+.0f%%\t%.1f\t%.1f\t%+.0f%%\t%s\n",
			s.Backend, s.Phase, human(s.Size), b.P95ms.Mean, s.P95ms.Mean, dp,
			b.OpsPerSec.Mean, s.OpsPerSec.Mean, do, verdict)
	}
	tw.Flush()
	if matched == 0 {
		fmt.Println("(nothing matched: baseline must cover the same backends, sizes and phases)")
	}
	return regressed, nil
}

// ------------------------------------------------------------------ helpers -

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	mult := int64(1)
	for _, u := range []struct {
		suf string
		m   int64
	}{{"gib", 1 << 30}, {"mib", 1 << 20}, {"kib", 1 << 10}, {"g", 1 << 30}, {"m", 1 << 20}, {"k", 1 << 10}, {"b", 1}} {
		if strings.HasSuffix(s, u.suf) {
			mult = u.m
			s = strings.TrimSuffix(s, u.suf)
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return n * mult, nil
}

func human(n int64) string {
	switch {
	case n >= 1<<30 && n%(1<<30) == 0:
		return fmt.Sprintf("%dGiB", n>>30)
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%dKiB", n>>10)
	}
	return fmt.Sprintf("%dB", n)
}

// newRunID is unique per invocation: a readable timestamp plus 48 random bits,
// so concurrent or back-to-back runs can never share a prefix.
func newRunID() string {
	b := make([]byte, 6)
	if _, err := crand.Read(b); err != nil {
		panic(err) // the OS random source failing is not recoverable
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b)
}

// rotate shifts the backend order by `by`, so with several runs each backend
// takes a turn going first and cache-warmth effects average out.
func rotate(names []string, by int) []string {
	out := make([]string, len(names))
	for i := range names {
		out[i] = names[(i+by)%len(names)]
	}
	return out
}

// newHTTP gives each backend its own connection pool so they don't influence
// each other. Timeouts are enforced per operation through contexts.
func newHTTP(conc int) *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: conc, DisableCompression: true}}
}

// --------------------------------------------------------------------- main -

func main() { os.Exit(run()) }

// run does the work and returns the exit status. It exists so deferred cleanup
// always runs (os.Exit would skip it).
func run() int {
	backendFlag := flag.String("backend", "", "REQUIRED: what to test: s3, filer, or s3,filer")
	s3Endpoint := flag.String("s3-endpoint", "http://127.0.0.1:8333", "S3 gateway endpoint")
	filerEndpoint := flag.String("filer-endpoint", "http://127.0.0.1:8888", "filer HTTP endpoint")
	bucket := flag.String("bucket", "perftest", "S3 bucket (created if missing); also the filer dir /buckets/<bucket>")
	filerDir := flag.String("filer-dir", "", "filer directory to write to (default /buckets/<bucket>)")
	sizesFlag := flag.String("sizes", "4KiB,1MiB", "comma-separated object sizes, e.g. 4KiB,1MiB,64MiB")
	count := flag.Int("count", 200, "objects per size, per run")
	conc := flag.Int("concurrency", 8, "parallel workers")
	runs := flag.Int("runs", 3, "repeat the whole benchmark this many times (needed for stddev)")
	warmup := flag.Int("warmup", 10, "untimed warmup objects per size")
	timeout := flag.Duration("timeout", 60*time.Second, "per-operation timeout; timeouts count as errors and as latency")
	seed := flag.Int64("seed", 1, "seed for payload generation (same seed = same bytes)")
	verify := flag.Bool("verify", false, "verify content hash on read (adds client CPU)")
	access := flag.String("access-key", os.Getenv("S3_ACCESS_KEY"), "S3 access key (optional)")
	secret := flag.String("secret-key", os.Getenv("S3_SECRET_KEY"), "S3 secret key (optional)")
	region := flag.String("region", "us-east-1", "region for S3 signing")
	label := flag.String("label", "", "free-form label stored in the results (e.g. fsync=on)")
	out := flag.String("out", "", "write results JSON to this file")
	baseline := flag.String("baseline", "", "compare against results JSON from a previous run")
	thr := flag.Float64("threshold", 20, "regression threshold in percent for -baseline")
	purge := flag.Bool("purge", false, "delete ALL seaweedperf data from the chosen backends and exit")
	flag.Parse()

	// --- validate arguments ---
	var names []string
	seen := map[string]bool{}
	for _, n := range strings.Split(*backendFlag, ",") {
		n = strings.TrimSpace(n)
		if n != "s3" && n != "filer" || seen[n] {
			fmt.Fprintln(os.Stderr, "choose what to test with -backend s3, -backend filer, or -backend s3,filer")
			return 2
		}
		seen[n] = true
		names = append(names, n)
	}
	var sizes []int64
	for _, s := range strings.Split(*sizesFlag, ",") {
		n, err := parseSize(s)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		sizes = append(sizes, n)
	}
	if *runs < 1 || *count < 1 || *conc < 1 {
		fmt.Fprintln(os.Stderr, "-runs, -count and -concurrency must be at least 1")
		return 2
	}

	// --- Ctrl-C / SIGTERM cancels the context; cleanup still runs afterwards ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Fprintln(os.Stderr, "\ninterrupted: stopping and cleaning up (Ctrl-C again to abort immediately)")
		cancel()
		signal.Stop(sigs) // a second signal now uses the default action and kills us
	}()

	dir := *filerDir
	if dir == "" {
		dir = "/buckets/" + *bucket
	}
	all := map[string]backend{
		"s3": newS3Backend(strings.TrimRight(*s3Endpoint, "/"), *bucket, *access, *secret, *region, newHTTP(*conc)),
		"filer": &filerBackend{
			base: strings.TrimRight(*filerEndpoint, "/"), dir: "/" + strings.Trim(dir, "/"), hc: newHTTP(*conc),
		},
	}

	cleanup := func(name string, be backend, prefix string) {
		// A fresh context: the main one may already be cancelled by Ctrl-C.
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		if err := be.RemovePrefix(cctx, prefix); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup %s failed: %v\n  leftovers are under %s; remove them with: seaweedperf -backend %s -purge\n",
				name, err, prefix, name)
		}
	}

	if *purge {
		status := 0
		for _, name := range names {
			fmt.Fprintf(os.Stderr, "purging %s data from %s...\n", rootPrefix, name)
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := all[name].RemovePrefix(cctx, rootPrefix); err != nil {
				fmt.Fprintf(os.Stderr, "purge %s: %v\n", name, err)
				status = 1
			}
			ccancel()
		}
		return status
	}

	// --- set up each backend, registering cleanup as soon as it could have data ---
	runID := newRunID()
	runPrefix := rootPrefix + runID + "/"
	for _, name := range names {
		if err := all[name].Setup(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "setup %s: %v\n", name, err)
			return 1
		}
		defer cleanup(name, all[name], runPrefix)
	}

	cfg := config{count: *count, conc: *conc, warmup: *warmup, timeout: *timeout, seed: *seed, verify: *verify}
	var perRun [][]result
	for ri := 0; ri < *runs && ctx.Err() == nil; ri++ {
		fmt.Fprintf(os.Stderr, "== run %d/%d ==\n", ri+1, *runs)
		prefix := fmt.Sprintf("%sr%d/", runPrefix, ri)
		var rr []result
		for _, name := range rotate(names, ri) {
			for _, size := range sizes {
				rr = append(rr, benchBackend(ctx, cfg, name, all[name], size, prefix)...)
			}
		}
		if ctx.Err() == nil { // never keep a partially-cancelled run
			perRun = append(perRun, rr)
		}
	}
	if ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "interrupted: partial results discarded")
		return 130
	}

	rep := report{
		Time: time.Now().UTC().Format(time.RFC3339), RunID: runID, Label: *label, Backends: names,
		S3Endpoint: *s3Endpoint, FilerEndpoint: *filerEndpoint, Bucket: *bucket,
		Concurrency: *conc, Count: *count, Runs: *runs, Verify: *verify, Seed: *seed,
		TimeoutSec: timeout.Seconds(), PerRun: perRun,
		Summary: summarize(names, sizes, perRun),
	}

	fmt.Printf("\n%s  run-id=%s backends=%s bucket=%s conc=%d count=%d runs=%d seed=%d label=%q\n",
		rep.Time, rep.RunID, strings.Join(names, ","), rep.Bucket, rep.Concurrency, rep.Count, rep.Runs, rep.Seed, rep.Label)
	if rep.Runs > 1 {
		fmt.Println("values are mean±stddev across runs; latency covers all attempts including failures")
	} else {
		fmt.Println("single run: no stddev (use -runs 3 or more); latency covers all attempts including failures")
	}
	printTable(rep)
	if len(names) > 1 {
		compareBackends(rep.Summary)
	}

	if *out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write results:", err)
			return 1
		}
		fmt.Println("\nresults written to", *out)
	}

	status := 0
	for _, s := range rep.Summary {
		if s.Errors > 0 {
			status = 1
		}
	}
	if *baseline != "" {
		b, err := os.ReadFile(*baseline)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read baseline:", err)
			return 1
		}
		var base report
		if err := json.Unmarshal(b, &base); err != nil {
			fmt.Fprintln(os.Stderr, "parse baseline:", err)
			return 1
		}
		regressed, err := compareBaseline(base, rep, *thr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "\n"+err.Error())
			return 2
		}
		if regressed {
			status = 1
		}
	}
	return status
}
