// perfsoak: a long-running, mixed read/write/delete soak test for the SeaweedFS S3
// gateway, with a configurable target rate for each operation type.
//
// Unlike seaweedperf (short bursts, one phase at a time), this keeps all three
// operations running at once for hours and records how latency and error rates
// evolve over time, so you can spot drift, stalls and slow degradation.
//
// How it works:
//
//   - Writes, reads and deletes each run on their own fixed schedule (-write-rate,
//     -read-rate, -delete-rate, in ops/s). The schedule does not wait for the server:
//     if the server slows down, operations pile up and latency grows, exactly as it
//     would for real clients. Latency is measured from when an operation was
//     SCHEDULED, so a stalled server cannot hide behind fewer, slower requests.
//   - If more than -max-inflight operations of one type are outstanding, further ones
//     are counted as "dropped": the system cannot sustain the target rate.
//   - Reads and deletes pick uniformly at random from the live object set, which is
//     first filled by -prefill-bytes, so reads hit a mix of old and recent data.
//   - Reads verify a CRC32C of the content (-verify) and any mismatch is reported as
//     corruption. This detects accidental corruption, not tampering.
//   - Total stored data is capped by -max-live-bytes / -max-live-objects. When writes
//     outpace deletes the set grows to the cap, after which writes are skipped.
//
// Output: a tidy CSV with one row per operation type per -interval (easy to plot with
// pandas), and a JSON summary with whole-run totals. Everything is written under
// perfsoak/<run-id>/ in the bucket and removed at exit, including after Ctrl-C.
//
// First time only (fetches the AWS SDK and writes go.sum):
//
//	go mod tidy && go build -o perfsoak .
//
// Examples:
//
//	./perfsoak -duration 4h -write-rate 20 -read-rate 60 -delete-rate 20
//	./perfsoak -duration 12h -write-rate 5 -read-rate 20 -delete-rate 5 -label overnight
//	./perfsoak -purge        # remove leftovers after a hard kill
//
// Exit status: 0 ok, 1 errors or corruption seen, 2 bad usage, 130 interrupted.
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// All soak data lives under this key prefix, so it is easy to find and purge.
const rootPrefix = "perfsoak/"

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	opNames    = []string{"write", "read", "delete"}
)

// ------------------------------------------------------------------- S3 -----

// s3c is a thin wrapper over the AWS SDK for Go v2.
type s3c struct {
	client *s3.Client
	bucket string
}

func newS3(endpoint, bucket, access, secret, region string, conns int) *s3c {
	var creds aws.CredentialsProvider = aws.AnonymousCredentials{}
	if access != "" {
		creds = credentials.NewStaticCredentialsProvider(access, secret, "")
	}
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: conns, DisableCompression: true}}
	cfg := aws.Config{Region: region, Credentials: creds, HTTPClient: hc}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// Newer SDKs add checksum trailers by default, which many S3-compatible
		// servers don't expect. Only send them when the API requires it.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &s3c{client: client, bucket: bucket}
}

func (c *s3c) setup(ctx context.Context) error {
	if _, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &c.bucket}); err == nil {
		return nil // already exists
	}
	_, err := c.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &c.bucket})
	return err
}

func (c *s3c) put(ctx context.Context, key string, data []byte) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &c.bucket, Key: &key, Body: bytes.NewReader(data)})
	return err
}

// get reads the whole object and returns its size and, when verify is set, its CRC32C.
func (c *s3c) get(ctx context.Context, key string, verify bool) (int64, uint32, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &c.bucket, Key: &key})
	if err != nil {
		return 0, 0, err
	}
	defer out.Body.Close()
	h := crc32.New(castagnoli)
	var dst io.Writer = io.Discard
	if verify {
		dst = h
	}
	n, err := io.Copy(dst, out.Body)
	if err != nil {
		return n, 0, err
	}
	return n, h.Sum32(), nil
}

func (c *s3c) del(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &c.bucket, Key: &key})
	return err
}

// removePrefix lists every object under prefix and deletes them in batches.
func (c *s3c) removePrefix(ctx context.Context, prefix string) error {
	pages := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{Bucket: &c.bucket, Prefix: &prefix})
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
		out, err := c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &c.bucket,
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

// ------------------------------------------------------------- live objects -

// obj is one object we have written and not yet deleted.
type obj struct {
	key     string
	size    int64
	crc     uint32
	deleted atomic.Bool // set just before the delete request, so late read failures aren't errors
}

// store tracks the live objects and enforces the size caps.
type store struct {
	mu       sync.Mutex
	rng      *rand.Rand
	objs     []*obj
	bytes    int64 // committed + reserved
	reserved int   // writes in flight
	maxBytes int64
	maxObjs  int
	minLive  int
}

// reserve claims room for a write. It fails when the caps would be exceeded.
func (s *store) reserve(size int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bytes+size > s.maxBytes || len(s.objs)+s.reserved >= s.maxObjs {
		return false
	}
	s.bytes += size
	s.reserved++
	return true
}

func (s *store) commit(o *obj) { // a reserved write succeeded
	s.mu.Lock()
	s.reserved--
	s.objs = append(s.objs, o)
	s.mu.Unlock()
}

func (s *store) abort(size int64) { // a reserved write failed or never ran
	s.mu.Lock()
	s.reserved--
	s.bytes -= size
	s.mu.Unlock()
}

// pick returns a random live object, or nil if there are none.
func (s *store) pick() *obj {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.objs) == 0 {
		return nil
	}
	return s.objs[s.rng.Intn(len(s.objs))]
}

// take removes and returns a random live object, or nil if only minLive remain.
func (s *store) take() *obj {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.objs) <= s.minLive {
		return nil
	}
	i := s.rng.Intn(len(s.objs))
	o := s.objs[i]
	last := len(s.objs) - 1
	s.objs[i] = s.objs[last]
	s.objs = s.objs[:last]
	return o
}

func (s *store) putBack(o *obj) { // a taken object was never deleted
	s.mu.Lock()
	s.objs = append(s.objs, o)
	s.mu.Unlock()
}

func (s *store) free(size int64) { // a delete finished
	s.mu.Lock()
	s.bytes -= size
	s.mu.Unlock()
}

func (s *store) snapshot() (objects int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objs), s.bytes
}

// ------------------------------------------------------------- size choice --

// sizeDist picks object sizes with relative weights, e.g. "8KiB:60,32KiB:30,64KiB:10".
type sizeDist struct {
	sizes   []int64
	weights []int
	total   int
}

func parseDist(spec string) (sizeDist, error) {
	var d sizeDist
	for _, part := range strings.Split(spec, ",") {
		sz, w, hasWeight := strings.Cut(strings.TrimSpace(part), ":")
		n, err := parseSize(sz)
		if err != nil {
			return d, err
		}
		weight := 1
		if hasWeight {
			if weight, err = strconv.Atoi(w); err != nil || weight <= 0 {
				return d, fmt.Errorf("bad weight in %q", part)
			}
		}
		d.sizes = append(d.sizes, n)
		d.weights = append(d.weights, weight)
		d.total += weight
	}
	return d, nil
}

func (d sizeDist) pick(rng *rand.Rand) int64 {
	x := rng.Intn(d.total)
	for i, w := range d.weights {
		if x < w {
			return d.sizes[i]
		}
		x -= w
	}
	return d.sizes[len(d.sizes)-1]
}

func (d sizeDist) max() int64 {
	var m int64
	for _, s := range d.sizes {
		if s > m {
			m = s
		}
	}
	return m
}

func (d sizeDist) mean() float64 {
	var sum float64
	for i, s := range d.sizes {
		sum += float64(s) * float64(d.weights[i])
	}
	return sum / float64(d.total)
}

// ---------------------------------------------------------------- statistics -

// hist is a log-spaced latency histogram (about 2% resolution, 1µs to ~45 min).
// It keeps memory constant however many hours the test runs.
type hist struct {
	counts [histBuckets]uint64
	n      uint64
	max    time.Duration
}

const (
	histBase    = 1.02
	histBuckets = 1100
)

func bucketOf(d time.Duration) int {
	us := float64(d) / 1e3
	if us < 1 {
		return 0
	}
	b := int(math.Log(us)/math.Log(histBase)) + 1
	if b >= histBuckets {
		b = histBuckets - 1
	}
	return b
}

func bucketUpper(b int) time.Duration {
	if b == 0 {
		return time.Microsecond
	}
	return time.Duration(math.Pow(histBase, float64(b)) * 1e3)
}

func (h *hist) add(d time.Duration) {
	h.counts[bucketOf(d)]++
	h.n++
	if d > h.max {
		h.max = d
	}
}

func (h *hist) quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(h.n)))
	var cum uint64
	for b, c := range h.counts[:] { // [:] avoids copying the array
		cum += c
		if cum >= target {
			if u := bucketUpper(b); u < h.max {
				return u
			}
			return h.max
		}
	}
	return h.max
}

// window accumulates counts for one time span (an interval, or the whole run).
type window struct {
	ok, errs, raced, corrupt, dropped, skipped, bytes int64
	h                                                 hist
}

// summary is a window turned into plain numbers.
type summary struct {
	OK      int64   `json:"ok"`
	Errors  int64   `json:"errors"`
	Raced   int64   `json:"raced"`   // reads that lost a race with a delete; not errors
	Corrupt int64   `json:"corrupt"` // reads whose content did not match what was written
	Dropped int64   `json:"dropped"` // not started: too many operations already in flight
	Skipped int64   `json:"skipped"` // not started: size cap reached, or nothing to read/delete
	Bytes   int64   `json:"bytes"`
	P50ms   float64 `json:"p50_ms"`
	P95ms   float64 `json:"p95_ms"`
	P99ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
}

// completed is the number of operations that actually ran and produced a result.
func (s summary) completed() int64 { return s.OK + s.Errors + s.Corrupt }

func (w *window) summarize() summary {
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return summary{
		OK: w.ok, Errors: w.errs, Raced: w.raced, Corrupt: w.corrupt,
		Dropped: w.dropped, Skipped: w.skipped, Bytes: w.bytes,
		P50ms: ms(w.h.quantile(0.50)), P95ms: ms(w.h.quantile(0.95)),
		P99ms: ms(w.h.quantile(0.99)), MaxMs: ms(w.h.max),
	}
}

type outcome int

const (
	outOK outcome = iota
	outErr
	outRaced
	outCorrupt
)

// opStats is the statistics for one operation type, safe for concurrent use.
type opStats struct {
	name    string
	target  float64
	mu      sync.Mutex
	iv, tot window
	newErrs []string // error messages seen since the last report (at most 3)
}

// record notes one finished operation. Latency counts for successes, errors and
// corruption alike, so slow failures show up in the percentiles; races are excluded.
func (s *opStats) record(kind outcome, lat time.Duration, bytes int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range []*window{&s.iv, &s.tot} {
		switch kind {
		case outOK:
			w.ok++
			w.bytes += bytes
		case outErr:
			w.errs++
		case outRaced:
			w.raced++
		case outCorrupt:
			w.corrupt++
		}
		if kind != outRaced {
			w.h.add(lat)
		}
	}
	if err != nil && len(s.newErrs) < 3 {
		s.newErrs = append(s.newErrs, err.Error())
	}
}

func (s *opStats) drop() {
	s.mu.Lock()
	s.iv.dropped++
	s.tot.dropped++
	s.mu.Unlock()
}

func (s *opStats) skip() {
	s.mu.Lock()
	s.iv.skipped++
	s.tot.skipped++
	s.mu.Unlock()
}

// roll returns the stats since the last roll and starts a new interval.
func (s *opStats) roll() (summary, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iv, errs := s.iv.summarize(), s.newErrs
	s.iv, s.newErrs = window{}, nil
	return iv, errs
}

func (s *opStats) total() summary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tot.summarize()
}

// -------------------------------------------------------------------- runner -

type config struct {
	duration, interval, timeout time.Duration
	maxInflight                 int
	verify                      bool
}

type runner struct {
	cfg    config
	s3     *s3c
	store  *store
	dist   sizeDist
	pool   []byte // random bytes; an object's content is a slice of it
	prefix string
	seq    atomic.Int64
	stats  map[string]*opStats
	sems   map[string]chan struct{} // limits operations in flight, per type
	wg     sync.WaitGroup           // operations in flight
	csv    *csv.Writer
	start  time.Time
}

// spawn runs f in a goroutine unless too many operations of this type are already
// in flight, in which case it counts a drop and returns false.
func (r *runner) spawn(st *opStats, f func()) bool {
	sem := r.sems[st.name]
	select {
	case sem <- struct{}{}:
	default:
		st.drop()
		return false
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-sem }()
		f()
	}()
	return true
}

// putObject uploads one object. Latency runs from `intended`, and is taken before
// the checksum is computed so hashing doesn't inflate it.
func (r *runner) putObject(intended time.Time, size int64, off int) (time.Duration, *obj, error) {
	n := r.seq.Add(1) - 1
	key := fmt.Sprintf("%s%06d/%03d", r.prefix, n/1000, n%1000) // 1000 objects per directory
	data := r.pool[off : off+int(size)]
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.timeout)
	defer cancel()
	if err := r.s3.put(ctx, key, data); err != nil {
		return time.Since(intended), nil, err
	}
	lat := time.Since(intended)
	return lat, &obj{key: key, size: size, crc: crc32.Checksum(data, castagnoli)}, nil
}

func (r *runner) doWrite(intended time.Time, size int64, off int) {
	st := r.stats["write"]
	lat, o, err := r.putObject(intended, size, off)
	if err != nil {
		r.store.abort(size)
		st.record(outErr, lat, 0, err)
		return
	}
	st.record(outOK, lat, size, nil)
	r.store.commit(o)
}

func (r *runner) doRead(intended time.Time, o *obj) {
	st := r.stats["read"]
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.timeout)
	defer cancel()
	n, sum, err := r.s3.get(ctx, o.key, r.cfg.verify)
	lat := time.Since(intended)
	var bad error
	switch {
	case err != nil && o.deleted.Load():
		st.record(outRaced, lat, 0, nil) // deleted while we were reading it
		return
	case err != nil:
		st.record(outErr, lat, 0, err)
		return
	case n != o.size:
		bad = fmt.Errorf("size mismatch on %s: got %d bytes, wrote %d", o.key, n, o.size)
	case r.cfg.verify && sum != o.crc:
		bad = fmt.Errorf("content mismatch on %s (checksum %08x, wrote %08x)", o.key, sum, o.crc)
	}
	if bad != nil {
		fmt.Fprintf(os.Stderr, "CORRUPTION: %v\n", bad)
		st.record(outCorrupt, lat, 0, bad)
		return
	}
	st.record(outOK, lat, n, nil)
}

func (r *runner) doDelete(intended time.Time, o *obj) {
	st := r.stats["delete"]
	o.deleted.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.timeout)
	defer cancel()
	err := r.s3.del(ctx, o.key)
	lat := time.Since(intended)
	r.store.free(o.size) // counted as gone even if the delete failed; cleanup removes leftovers
	if err != nil {
		st.record(outErr, lat, 0, err)
		return
	}
	st.record(outOK, lat, 0, nil)
}

// pace calls fire on a fixed schedule of `rate` per second until ctx ends. The
// schedule never waits for fire's work to complete (open loop).
func pace(ctx context.Context, rate float64, fire func(intended time.Time)) {
	interval := time.Duration(float64(time.Second) / rate)
	next := time.Now()
	for {
		if d := time.Until(next); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		} else if ctx.Err() != nil {
			return
		}
		fire(next)
		next = next.Add(interval)
		if time.Since(next) > 5*time.Second { // e.g. the machine was suspended: don't fire a huge burst
			next = time.Now()
		}
	}
}

// startStreams launches one pacer per operation type with a non-zero rate.
func (r *runner) startStreams(ctx context.Context, seed int64, wg *sync.WaitGroup) {
	launch := func(name string, fire func(time.Time)) {
		if r.stats[name].target <= 0 {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			pace(ctx, r.stats[name].target, fire)
		}()
	}

	wrng := rand.New(rand.NewSource(seed + 1)) // used only by the write pacer goroutine
	launch("write", func(intended time.Time) {
		st := r.stats["write"]
		size := r.dist.pick(wrng)
		if !r.store.reserve(size) {
			st.skip()
			return
		}
		off := wrng.Intn(len(r.pool) - int(size) + 1)
		if !r.spawn(st, func() { r.doWrite(intended, size, off) }) {
			r.store.abort(size)
		}
	})
	launch("read", func(intended time.Time) {
		st := r.stats["read"]
		o := r.store.pick()
		if o == nil {
			st.skip()
			return
		}
		r.spawn(st, func() { r.doRead(intended, o) })
	})
	launch("delete", func(intended time.Time) {
		st := r.stats["delete"]
		o := r.store.take()
		if o == nil {
			st.skip()
			return
		}
		if !r.spawn(st, func() { r.doDelete(intended, o) }) {
			r.store.putBack(o)
		}
	})
}

// prefill writes objects (untimed) until the live set holds about `target` bytes.
func (r *runner) prefill(ctx context.Context, target int64, conc int, seed int64) {
	rng := rand.New(rand.NewSource(seed + 2))
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed atomic.Int64
	lastPrint := time.Now()
	for ctx.Err() == nil {
		if _, b := r.store.snapshot(); b >= target {
			break
		}
		size := r.dist.pick(rng)
		if !r.store.reserve(size) {
			break // hit the caps
		}
		off := rng.Intn(len(r.pool) - int(size) + 1)
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_, o, err := r.putObject(time.Now(), size, off)
			if err != nil {
				if failed.Add(1) <= 3 {
					fmt.Fprintf(os.Stderr, "  prefill error: %v\n", err)
				}
				r.store.abort(size)
				return
			}
			r.store.commit(o)
		}()
		if time.Since(lastPrint) > 10*time.Second {
			n, b := r.store.snapshot()
			fmt.Printf("  prefill: %d objects, %.2f GiB\n", n, gib(b))
			lastPrint = time.Now()
		}
	}
	wg.Wait()
	n, b := r.store.snapshot()
	fmt.Printf("prefill done: %d objects, %.2f GiB (%d failed)\n", n, gib(b), failed.Load())
}

// ----------------------------------------------------------------- reporting -

func gib(b int64) float64 { return float64(b) / (1 << 30) }

// emit writes one CSV row per operation type for the interval since `since`,
// prints a one-line status, and returns the time the interval ended.
func (r *runner) emit(since time.Time) time.Time {
	now := time.Now()
	dt := now.Sub(since).Seconds()
	if dt <= 0 {
		dt = 1e-9
	}
	objects, bytes := r.store.snapshot()
	elapsed := now.Sub(r.start).Seconds()
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	i := func(v int64) string { return strconv.FormatInt(v, 10) }

	line := fmt.Sprintf("%7.0fs live=%d/%.2fGiB", elapsed, objects, gib(bytes))
	for _, name := range opNames {
		st := r.stats[name]
		iv, errs := st.roll()
		achieved := float64(iv.completed()) / dt
		r.csv.Write([]string{
			now.UTC().Format(time.RFC3339), f(elapsed), name, f(st.target), f(achieved),
			i(iv.OK), i(iv.Errors), i(iv.Raced), i(iv.Corrupt), i(iv.Dropped), i(iv.Skipped),
			f(iv.P50ms), f(iv.P95ms), f(iv.P99ms), f(iv.MaxMs), f(float64(iv.Bytes) / 1e6 / dt),
			i(int64(objects)), f(gib(bytes)), i(int64(len(r.sems[name]))),
		})
		line += fmt.Sprintf(" | %c %.1f/%.0f p95=%.0fms", name[0], achieved, st.target, iv.P95ms)
		if bad := iv.Errors + iv.Corrupt; bad > 0 {
			line += fmt.Sprintf(" err=%d", bad)
		}
		if iv.Dropped > 0 {
			line += fmt.Sprintf(" drop=%d", iv.Dropped)
		}
		if iv.Skipped > 0 {
			line += fmt.Sprintf(" skip=%d", iv.Skipped)
		}
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "  [%s] error: %s\n", name, e)
		}
	}
	r.csv.Flush() // flushed every interval so a killed run keeps its data
	fmt.Println(line)
	return now
}

func (r *runner) reportLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(r.cfg.interval)
	defer t.Stop()
	last := r.start
	for {
		select {
		case <-t.C:
			last = r.emit(last)
		case <-stop:
			r.emit(last) // the final, partial interval
			return
		}
	}
}

type opReport struct {
	TargetOpsPerSec   float64 `json:"target_ops_per_sec"`
	AchievedOpsPerSec float64 `json:"achieved_ops_per_sec"`
	Totals            summary `json:"totals"`
}

type finalReport struct {
	RunID       string              `json:"run_id"`
	Label       string              `json:"label"`
	Start       string              `json:"start"`
	End         string              `json:"end"`
	SoakSeconds float64             `json:"soak_seconds"`
	Interrupted bool                `json:"interrupted"`
	Flags       map[string]string   `json:"flags"`
	Ops         map[string]opReport `json:"ops"`
}

// ------------------------------------------------------------------- helpers -

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

// newRunID is unique per invocation: a readable timestamp plus 48 random bits.
func newRunID() string {
	b := make([]byte, 6)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b)
}

// --------------------------------------------------------------------- main -

func main() { os.Exit(run()) }

// run does the work and returns the exit status, so deferred cleanup always runs.
func run() int {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8333", "S3 gateway endpoint")
	bucket := flag.String("bucket", "perftest", "S3 bucket (shares seaweedfs volumes with seaweedperf if the same)")
	access := flag.String("access-key", os.Getenv("S3_ACCESS_KEY"), "S3 access key (optional)")
	secret := flag.String("secret-key", os.Getenv("S3_SECRET_KEY"), "S3 secret key (optional)")
	region := flag.String("region", "us-east-1", "region for S3 signing")

	duration := flag.Duration("duration", 1*time.Hour, "how long to run the soak, e.g. 4h or 90m")
	writeRate := flag.Float64("write-rate", 20, "target writes per second (0 disables)")
	readRate := flag.Float64("read-rate", 50, "target reads per second (0 disables)")
	deleteRate := flag.Float64("delete-rate", 20, "target deletes per second (0 disables)")
	sizesFlag := flag.String("sizes", "8KiB:60,32KiB:30,64KiB:10", "object sizes with relative weights, e.g. 8KiB:60,64KiB:40")
	prefillFlag := flag.String("prefill-bytes", "1GiB", "data to write (untimed) before the soak so reads and deletes have a population")
	maxBytesFlag := flag.String("max-live-bytes", "5GiB", "cap on stored data; writes are skipped at the cap")
	maxObjs := flag.Int("max-live-objects", 500000, "cap on stored objects; writes are skipped at the cap")
	minLive := flag.Int("min-live", 100, "deletes are skipped when only this many objects remain")
	maxInflight := flag.Int("max-inflight", 64, "max outstanding operations per type; more are counted as dropped")
	timeout := flag.Duration("timeout", 60*time.Second, "per-operation timeout; timeouts count as errors and as latency")
	verify := flag.Bool("verify", true, "verify content checksums on read")
	seed := flag.Int64("seed", 1, "seed for payload bytes and size choices")

	interval := flag.Duration("interval", 30*time.Second, "how often to write a CSV row and a status line")
	label := flag.String("label", "", "free-form label stored in the results")
	out := flag.String("out", "", "output file base name; writes <out>.csv and <out>.json (default perfsoak-<run-id>)")
	purge := flag.Bool("purge", false, "delete ALL perfsoak data from the bucket and exit")
	flag.Parse()

	// --- validate ---
	dist, err := parseDist(*sizesFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "-sizes:", err)
		return 2
	}
	prefillBytes, err1 := parseSize(*prefillFlag)
	maxBytes, err2 := parseSize(*maxBytesFlag)
	if err1 != nil || err2 != nil {
		fmt.Fprintln(os.Stderr, "-prefill-bytes and -max-live-bytes must be sizes like 1GiB")
		return 2
	}
	if *writeRate < 0 || *readRate < 0 || *deleteRate < 0 || (*writeRate == 0 && *readRate == 0 && *deleteRate == 0) {
		fmt.Fprintln(os.Stderr, "rates must not be negative, and at least one must be above zero")
		return 2
	}
	if *maxInflight < 1 || *interval <= 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "-max-inflight, -interval and -duration must be positive")
		return 2
	}

	// --- Ctrl-C / SIGTERM stops the soak early; results so far are still written ---
	sigCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Fprintln(os.Stderr, "\ninterrupted: finishing in-flight operations, then cleaning up (Ctrl-C again to abort)")
		cancel()
		signal.Stop(sigs) // a second signal now uses the default action and kills us
	}()

	c := newS3(strings.TrimRight(*endpoint, "/"), *bucket, *access, *secret, *region, *maxInflight*3+16)

	if *purge {
		fmt.Fprintf(os.Stderr, "purging %s from bucket %s...\n", rootPrefix, *bucket)
		ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		if err := c.removePrefix(ctx, rootPrefix); err != nil {
			fmt.Fprintln(os.Stderr, "purge:", err)
			return 1
		}
		return 0
	}

	// --- set up ---
	runID := newRunID()
	base := *out
	if base == "" {
		base = "perfsoak-" + runID
	}
	csvFile, err := os.Create(base + ".csv")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create csv:", err)
		return 1
	}
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	w.Write([]string{"time", "elapsed_s", "op", "target_ops_s", "achieved_ops_s", "ok", "errors", "raced", "corrupt",
		"dropped", "skipped", "p50_ms", "p95_ms", "p99_ms", "max_ms", "mb_s", "live_objects", "live_gib", "inflight"})
	w.Flush()

	if err := c.setup(sigCtx); err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		return 1
	}
	runPrefix := rootPrefix + runID + "/"
	defer func() {
		// A fresh context: the main one is already cancelled after Ctrl-C.
		ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		fmt.Fprintf(os.Stderr, "cleaning up %s...\n", runPrefix)
		if err := c.removePrefix(ctx, runPrefix); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup failed: %v\n  leftovers are under %s; remove them with: perfsoak -purge\n", err, runPrefix)
		}
	}()

	pool := make([]byte, dist.max()+(1<<20))
	rand.New(rand.NewSource(*seed)).Read(pool)

	rates := map[string]float64{"write": *writeRate, "read": *readRate, "delete": *deleteRate}
	r := &runner{
		cfg:    config{duration: *duration, interval: *interval, timeout: *timeout, maxInflight: *maxInflight, verify: *verify},
		s3:     c,
		dist:   dist,
		pool:   pool,
		prefix: runPrefix,
		stats:  map[string]*opStats{},
		sems:   map[string]chan struct{}{},
		csv:    w,
		store: &store{
			rng: rand.New(rand.NewSource(*seed + 3)), maxBytes: maxBytes, maxObjs: *maxObjs, minLive: *minLive,
		},
	}
	for _, name := range opNames {
		r.stats[name] = &opStats{name: name, target: rates[name]}
		r.sems[name] = make(chan struct{}, *maxInflight)
	}

	// --- tell the user what to expect ---
	mean := dist.mean()
	fmt.Printf("run %s -> %s.csv / %s.json\n", runID, base, base)
	fmt.Printf("targets: write %.1f/s, read %.1f/s, delete %.1f/s; mean object %.0f KiB; duration %s\n",
		*writeRate, *readRate, *deleteRate, mean/1024, *duration)
	fmt.Printf("expected bandwidth: writes %.2f MB/s, reads %.2f MB/s\n", *writeRate*mean/1e6, *readRate*mean/1e6)
	if growth := (*writeRate - *deleteRate) * mean; growth > 0 {
		hours := float64(maxBytes-prefillBytes) / growth / 3600
		fmt.Printf("note: writes outpace deletes; the live set grows by %.2f MB/s and reaches the %s cap in about %.1f h,\n"+
			"      after which writes are skipped. Raise -delete-rate to hold steady.\n", growth/1e6, human(maxBytes), math.Max(hours, 0))
	} else if growth < 0 {
		fmt.Printf("note: deletes outpace writes; the live set will drain to -min-live (%d objects).\n", *minLive)
	}

	// --- prefill, then soak ---
	fmt.Printf("prefilling %s...\n", human(prefillBytes))
	r.prefill(sigCtx, prefillBytes, 16, *seed)

	status := 0
	r.start = time.Now()
	soakCtx, stopSoak := context.WithTimeout(sigCtx, *duration)
	defer stopSoak()
	if sigCtx.Err() == nil {
		fmt.Println("soak started")
		stop, reportDone := make(chan struct{}), make(chan struct{})
		go r.reportLoop(stop, reportDone)
		var pacers sync.WaitGroup
		r.startStreams(soakCtx, *seed, &pacers)
		pacers.Wait() // returns when the duration ends or Ctrl-C
		r.wg.Wait()   // let in-flight operations finish
		close(stop)
		<-reportDone
	}
	interrupted := sigCtx.Err() != nil
	soakSeconds := time.Since(r.start).Seconds()

	// --- summary ---
	rep := finalReport{
		RunID: runID, Label: *label, Start: r.start.UTC().Format(time.RFC3339),
		End: time.Now().UTC().Format(time.RFC3339), SoakSeconds: soakSeconds,
		Interrupted: interrupted, Flags: map[string]string{}, Ops: map[string]opReport{},
	}
	flag.VisitAll(func(f *flag.Flag) { rep.Flags[f.Name] = f.Value.String() })
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nOP\tTARGET/s\tACHIEVED/s\tOK\tERR\tCORRUPT\tRACED\tDROPPED\tSKIPPED\tP50ms\tP95ms\tP99ms\tMAXms")
	for _, name := range opNames {
		st := r.stats[name]
		t := st.total()
		achieved := float64(t.completed()) / soakSeconds
		rep.Ops[name] = opReport{TargetOpsPerSec: st.target, AchievedOpsPerSec: achieved, Totals: t}
		fmt.Fprintf(tw, "%s\t%.1f\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\n",
			name, st.target, achieved, t.OK, t.Errors, t.Corrupt, t.Raced, t.Dropped, t.Skipped,
			t.P50ms, t.P95ms, t.P99ms, t.MaxMs)
		if t.Errors > 0 || t.Corrupt > 0 {
			status = 1
		}
	}
	tw.Flush()
	for _, name := range opNames {
		st, t := r.stats[name], rep.Ops[name]
		if st.target > 0 && t.AchievedOpsPerSec < 0.95*st.target {
			fmt.Printf("WARNING: %s achieved %.1f/s, under 95%% of the %.1f/s target (%d dropped, %d skipped)\n",
				name, t.AchievedOpsPerSec, st.target, t.Totals.Dropped, t.Totals.Skipped)
		}
		if t.Totals.Corrupt > 0 {
			fmt.Printf("CORRUPTION DETECTED: %d %s operations returned data that did not match what was written\n",
				t.Totals.Corrupt, name)
		}
	}
	if b, err := json.MarshalIndent(rep, "", "  "); err == nil {
		if err := os.WriteFile(base+".json", b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write summary:", err)
			status = 1
		} else {
			fmt.Printf("\nwrote %s.csv and %s.json\n", base, base)
		}
	}
	if interrupted {
		return 130
	}
	return status
}
