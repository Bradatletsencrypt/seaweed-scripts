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
//   - The S3 client does NOT retry: every failure the server causes is counted. The
//     connection pool is sized so connections are reused rather than re-dialled.
//   - If more than -max-inflight operations of one type are outstanding, further ones
//     are "dropped": the system cannot sustain the target rate. By default each
//     dropped operation is credited with a latency of -timeout in the percentiles
//     (-credit-dropped), so a saturated system cannot look healthy; the dropped
//     column says how many were credited. Operations the scheduler itself missed
//     (the machine was suspended or overloaded) are counted as dropped but NOT
//     credited, since that is the test machine's fault, not the server's.
//   - Reads and deletes pick from the live object set, which is first filled by
//     -prefill-bytes. Reads are uniform by default; -read-skew gives them a
//     Zipf-like hot/cold pattern.
//   - Reads verify a CRC32C of the content (-verify) and any mismatch is reported as
//     corruption. A read that overlaps a delete of the same object is never counted
//     as corruption (it is "raced"). This detects accidental corruption, not tampering.
//   - Latency is also recorded per object-size class, so a regression in large
//     objects is not hidden behind the small ones.
//   - Every -list-check-interval one directory of objects is listed and compared with
//     what the test believes is stored, to catch listing drift, missing objects and
//     deletes that did not stick.
//   - Optionally (-scrape-urls) SeaweedFS /metrics endpoints are sampled every
//     interval into their own CSV so client latency can be lined up with server state
//     such as vacuum, compaction and disk usage.
//   - Total stored data is capped by -max-live-bytes / -max-live-objects. When writes
//     outpace deletes the set grows to the cap, after which writes are skipped.
//
// Output (base name from -out, default perfsoak-<run-id>):
//
//	<out>.csv         one row per operation type per -interval (plot with plot_soak.py)
//	<out>-sizes.csv   the same, per operation type and object size
//	<out>-server.csv  sampled server metrics (only with -scrape-urls), long format
//	<out>.json        whole-run totals and the flags used
//
// Everything is written under perfsoak/<run-id>/ in the bucket and removed at exit,
// including after Ctrl-C.
//
// First time only (fetches the AWS SDK and writes go.sum):
//
//	go mod tidy && go build -o perfsoak .
//
// Examples:
//
//	./perfsoak -duration 4h -write-rate 20 -read-rate 60 -delete-rate 20
//	./perfsoak -duration 12h -read-skew 1.2 -scrape-urls http://seaweed:9325/metrics
//	./perfsoak -purge        # remove leftovers from runs older than -purge-older-than
//
// Exit status: 0 ok; 1 errors, corruption, listing mismatches or output-file
// failures were seen (even if the run was interrupted); 2 bad usage; 130 interrupted
// with nothing wrong.
package main

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"net"
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
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// All soak data lives under this key prefix, so it is easy to find and purge.
const rootPrefix = "perfsoak/"

// A prefill that fails this many times in a row gives up: the server is down or
// rejecting writes, and retrying forever would just spin.
const prefillMaxConsecutiveFailures = 20

// Objects are stored 1000 per "directory": <prefix>NNNNNN/NNN.
const shardSize = 1000

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	opNames    = []string{"write", "read", "delete"}
)

// Lifecycle of an object, tracked per sequence number so listings can be checked.
const (
	stNone     uint8 = iota // never allocated
	stWriting               // upload in progress
	stLive                  // stored and readable
	stDeleting              // delete in progress (or about to start)
	stDeleted               // delete succeeded
	stUnknown               // a write or delete failed: it may or may not exist
)

// ------------------------------------------------------------------- S3 -----

// s3c is a thin wrapper over the AWS SDK for Go v2.
type s3c struct {
	client *s3.Client
	bucket string
}

// newS3 builds a client. The workload client must not retry (retry=false) so that
// transient failures are visible; the admin client used for setup, cleanup and
// listing does retry, since there a retry is helpful and not being measured.
func newS3(endpoint, bucket, access, secret, region string, conns int, retry bool) *s3c {
	var creds aws.CredentialsProvider = aws.AnonymousCredentials{}
	if access != "" {
		creds = credentials.NewStaticCredentialsProvider(access, secret, "")
	}
	// Idle and total limits are all `conns`, otherwise the transport closes and
	// re-dials connections under load, adding TCP setup to latency and piling up
	// TIME_WAIT sockets over a long run.
	hc := &http.Client{Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        conns,
		MaxIdleConnsPerHost: conns,
		MaxConnsPerHost:     conns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}}
	cfg := aws.Config{Region: region, Credentials: creds, HTTPClient: hc}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// Newer SDKs add checksum trailers by default, which many S3-compatible
		// servers don't expect. Only send them when the API requires it.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if !retry {
			o.Retryer = aws.NopRetryer{}
		}
	})
	return &s3c{client: client, bucket: bucket}
}

// isNotFound reports whether err means "no such bucket/object" rather than a
// failure (bad credentials, network error, ...).
func isNotFound(err error) bool {
	var nf *types.NotFound
	var nsb *types.NoSuchBucket
	if errors.As(err, &nf) || errors.As(err, &nsb) {
		return true
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NotFound", "NoSuchBucket", "404":
			return true
		}
	}
	return false
}

// setup makes sure the bucket exists. It only creates it when it is definitely
// missing, so a permissions or network problem is reported as such instead of as a
// confusing create-bucket error.
func (c *s3c) setup(ctx context.Context) error {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &c.bucket})
	if err == nil {
		return nil
	}
	if !isNotFound(err) {
		return fmt.Errorf("checking bucket %q: %w", c.bucket, err)
	}
	_, err = c.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &c.bucket})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &owned) {
			return nil // created by someone else a moment ago
		}
		return fmt.Errorf("creating bucket %q: %w", c.bucket, err)
	}
	return nil
}

// connectHint explains the usual causes of a failed connection in plain words.
func connectHint(err error, endpoint, bucket string) string {
	probe := fmt.Sprintf("curl -sI %s/%s   (a healthy gateway answers 200 or 404)", strings.TrimRight(endpoint, "/"), bucket)
	switch {
	case errors.Is(err, syscall.ECONNRESET):
		return "hint: the server accepted the connection and then dropped it. The gateway is probably restarting,\n" +
			"      overloaded (e.g. by an earlier run still running: pgrep -a perfsoak), or out of file descriptors.\n" +
			"      Check it directly: " + probe
	case errors.Is(err, syscall.ECONNREFUSED):
		return "hint: nothing is listening at " + endpoint + ". Is the S3 gateway running, and is the port right?"
	}
	return "hint: check that the gateway is reachable: " + probe
}

// waitForBucket makes sure the bucket exists, retrying for up to `wait` so a gateway
// that is restarting or briefly overloaded does not fail the run straight away.
func waitForBucket(ctx context.Context, c *s3c, endpoint string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	delay := time.Second
	for attempt := 1; ; attempt++ {
		err := c.setup(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Add(delay).After(deadline) {
			return fmt.Errorf("%w\n%s", err, connectHint(err, endpoint, c.bucket))
		}
		fmt.Fprintf(os.Stderr, "S3 gateway not ready (attempt %d): %v\n  retrying in %s...\n", attempt, err, delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		if delay < 8*time.Second {
			delay *= 2
		}
	}
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

// listKeys returns every key under prefix.
func (c *s3c) listKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pages := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{Bucket: &c.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

// listRunIDs returns the run ids present under rootPrefix.
func (c *s3c) listRunIDs(ctx context.Context) ([]string, error) {
	var ids []string
	pages := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: &c.bucket, Prefix: aws.String(rootPrefix), Delimiter: aws.String("/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			id := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), rootPrefix), "/")
			if id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

// removePrefix deletes every object under prefix. It uses batch deletes, and falls
// back to one-by-one deletes if the server rejects them (some S3-compatible servers
// are picky about the checksum headers batch deletes need).
func (c *s3c) removePrefix(ctx context.Context, prefix string) error {
	batchOK := true
	pages := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{Bucket: &c.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}
		keys := make([]string, 0, len(page.Contents))
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		if batchOK {
			err := c.deleteBatch(ctx, keys)
			if err == nil {
				continue
			}
			fmt.Fprintf(os.Stderr, "batch delete failed (%v); falling back to single deletes\n", err)
			batchOK = false
		}
		if err := c.deleteEach(ctx, keys, 16); err != nil {
			return err
		}
	}
	return nil
}

func (c *s3c) deleteBatch(ctx context.Context, keys []string) error {
	ids := make([]types.ObjectIdentifier, 0, len(keys))
	for i := range keys {
		ids = append(ids, types.ObjectIdentifier{Key: &keys[i]})
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
	return nil
}

func (c *s3c) deleteEach(ctx context.Context, keys []string, conc int) error {
	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range jobs {
				if err := c.del(ctx, k); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, k := range keys {
		jobs <- k
	}
	close(jobs)
	wg.Wait()
	return first
}

// runStart extracts the start time from a run id (<20060102T150405>-<hex>).
func runStart(id string) (time.Time, bool) {
	if len(id) < 15 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102T150405", id[:15])
	return t, err == nil
}

// purge removes leftovers from earlier runs. Runs younger than olderThan are
// kept, so purging cannot delete the data of a soak that is still running.
func purge(ctx context.Context, c *s3c, olderThan time.Duration) int {
	ids, err := c.listRunIDs(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "purge:", err)
		return 1
	}
	status := 0
	for _, id := range ids {
		t, ok := runStart(id)
		if ok && olderThan > 0 && time.Since(t) < olderThan {
			fmt.Printf("keeping %s (started %s ago; -purge-older-than=%s)\n", id, time.Since(t).Round(time.Minute), olderThan)
			continue
		}
		fmt.Printf("removing %s\n", id)
		if err := c.removePrefix(ctx, rootPrefix+id+"/"); err != nil {
			fmt.Fprintf(os.Stderr, "purge %s: %v\n", id, err)
			status = 1
		}
	}
	if len(ids) == 0 {
		fmt.Println("nothing to purge")
	}
	return status
}

// ------------------------------------------------------------- live objects -

// obj is one object we have written and not yet deleted.
type obj struct {
	key     string
	n       int64 // sequence number; the key is derived from it
	size    int64
	crc     uint32
	deleted atomic.Bool // set just before the delete request, so overlapping reads aren't errors
}

// store tracks the live objects, enforces the size caps, and remembers each
// object's lifecycle state so listings can be checked against it.
type store struct {
	mu       sync.Mutex
	rng      *rand.Rand
	objs     []*obj
	states   []uint8 // indexed by sequence number
	bytes    int64   // committed + reserved
	reserved int     // writes in flight
	maxBytes int64
	maxObjs  int
	minLive  int
}

func (s *store) setLocked(n int64, st uint8) {
	for int64(len(s.states)) <= n {
		s.states = append(s.states, stNone)
	}
	s.states[n] = st
}

func (s *store) setState(n int64, st uint8) {
	s.mu.Lock()
	s.setLocked(n, st)
	s.mu.Unlock()
}

// statesRange returns a copy of the states for sequence numbers [lo, hi).
func (s *store) statesRange(lo, hi int64) []uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint8, hi-lo)
	for i := lo; i < hi && i < int64(len(s.states)); i++ {
		out[i-lo] = s.states[i]
	}
	return out
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
	s.setLocked(o.n, stLive)
	s.mu.Unlock()
}

func (s *store) abort(size int64) { // a reserved write failed or never ran
	s.mu.Lock()
	s.reserved--
	s.bytes -= size
	s.mu.Unlock()
}

// pick returns a live object, or nil if there are none. With skew <= 1 the choice is
// uniform; with skew > 1 it follows a Zipf distribution over positions in the live
// list, so low positions are read far more often. A position keeps its rank, but
// when its object is deleted the newest object takes its place, so the hot set
// slowly turns over as deletes happen.
func (s *store) pick(skew float64) *obj {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.objs)
	if n == 0 {
		return nil
	}
	if skew > 1 {
		return s.objs[rand.NewZipf(s.rng, skew, 1, uint64(n-1)).Uint64()]
	}
	return s.objs[s.rng.Intn(n)]
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
	s.setLocked(o.n, stDeleting)
	return o
}

func (s *store) putBack(o *obj) { // a taken object was never deleted
	s.mu.Lock()
	s.objs = append(s.objs, o)
	s.setLocked(o.n, stLive)
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
	Raced   int64   `json:"raced"`   // reads that overlapped a delete of the same object; not errors
	Corrupt int64   `json:"corrupt"` // reads whose content did not match what was written
	Dropped int64   `json:"dropped"` // not started: too many in flight, or the scheduler fell behind
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

// sizeSummary is the totals for one object-size class.
type sizeSummary struct {
	SizeBytes int64   `json:"size_bytes"`
	Totals    summary `json:"totals"`
}

// opStats is the statistics for one operation type, safe for concurrent use. Besides
// the overall windows it keeps one window per object size.
type opStats struct {
	name      string
	target    float64
	credit    time.Duration // latency credited to an operation dropped for lack of capacity; 0 = none
	sizes     []int64
	sizeIdx   map[int64]int
	mu        sync.Mutex
	iv, tot   window
	ivS, totS []window
	newErrs   []string // error messages seen since the last report (at most 3)
}

func newOpStats(name string, target float64, credit time.Duration, sizes []int64) *opStats {
	s := &opStats{
		name: name, target: target, credit: credit, sizes: sizes, sizeIdx: map[int64]int{},
		ivS: make([]window, len(sizes)), totS: make([]window, len(sizes)),
	}
	for i, sz := range sizes {
		if _, dup := s.sizeIdx[sz]; !dup {
			s.sizeIdx[sz] = i
		}
	}
	return s
}

// windows returns every window an event of this object size belongs to.
func (s *opStats) windows(size int64) []*window {
	w := []*window{&s.iv, &s.tot}
	if i, ok := s.sizeIdx[size]; ok {
		w = append(w, &s.ivS[i], &s.totS[i])
	}
	return w
}

// record notes one finished operation. Latency counts for successes, errors and
// corruption alike, so slow failures show up in the percentiles; races are excluded.
func (s *opStats) record(kind outcome, lat time.Duration, bytes, size int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.windows(size) {
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

// drop notes an operation that could not start because too many were in flight.
// When crediting is on it also counts as taking `credit` in the latency percentiles,
// so a saturated system cannot look healthy just because it stopped being asked.
func (s *opStats) drop(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.windows(size) {
		w.dropped++
		if s.credit > 0 {
			w.h.add(s.credit)
		}
	}
}

// dropN notes n operations the scheduler never fired (no latency credit).
func (s *opStats) dropN(n int64) {
	s.mu.Lock()
	s.iv.dropped += n
	s.tot.dropped += n
	s.mu.Unlock()
}

func (s *opStats) skip() {
	s.mu.Lock()
	s.iv.skipped++
	s.tot.skipped++
	s.mu.Unlock()
}

// rolled is what roll returns: the interval's stats overall and per size.
type rolled struct {
	iv     summary
	bySize []sizeSummary
	errs   []string
}

// roll returns the stats since the last roll and starts a new interval.
func (s *opStats) roll() rolled {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := rolled{iv: s.iv.summarize(), errs: s.newErrs}
	for i, sz := range s.sizes {
		if s.sizeIdx[sz] == i { // skip duplicate size entries
			r.bySize = append(r.bySize, sizeSummary{SizeBytes: sz, Totals: s.ivS[i].summarize()})
		}
		s.ivS[i] = window{}
	}
	s.iv, s.newErrs = window{}, nil
	return r
}

func (s *opStats) total() (summary, []sizeSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var by []sizeSummary
	for i, sz := range s.sizes {
		if s.sizeIdx[sz] == i {
			by = append(by, sizeSummary{SizeBytes: sz, Totals: s.totS[i].summarize()})
		}
	}
	return s.tot.summarize(), by
}

// ---------------------------------------------------------------- CSV output -

// csvOut is a CSV file that remembers its first write failure (a full disk over a
// long run is plausible) so it can be reported instead of silently losing data.
type csvOut struct {
	name   string
	f      *os.File
	w      *csv.Writer
	err    error
	closed bool
}

func newCSV(path string, header []string) (*csvOut, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	c := &csvOut{name: path, f: f, w: csv.NewWriter(f)}
	c.w.Write(header)
	c.flush()
	return c, c.err
}

func (c *csvOut) write(rec []string) { c.w.Write(rec) } // failures surface in flush

func (c *csvOut) flush() {
	c.w.Flush()
	if err := c.w.Error(); err != nil && c.err == nil {
		c.err = err
		fmt.Fprintf(os.Stderr, "WARNING: writing %s failed: %v (disk full?); the rest of this file may be missing\n", c.name, err)
	}
}

func (c *csvOut) close() {
	if c.closed { // closed explicitly and again by a defer
		return
	}
	c.closed = true
	c.flush()
	if err := c.f.Close(); err != nil && c.err == nil {
		c.err = err
		fmt.Fprintf(os.Stderr, "WARNING: closing %s failed: %v\n", c.name, err)
	}
}

// -------------------------------------------------------------------- runner -

type config struct {
	interval, timeout time.Duration
	verify            bool
	readSkew          float64
}

type listStats struct {
	checks, errors, missing, undeleted, foreign atomic.Int64
}

type runner struct {
	cfg      config
	s3       *s3c // workload client: no retries
	admin    *s3c // listing client: retries
	store    *store
	dist     sizeDist
	pool     []byte // random bytes; an object's content is a slice of it
	prefix   string
	seq      atomic.Int64
	stats    map[string]*opStats
	sems     map[string]chan struct{} // limits operations in flight, per type
	wg       sync.WaitGroup           // operations in flight
	out      *csvOut                  // per-interval rows
	sizesOut *csvOut                  // per-interval, per-size rows
	lc       listStats
	start    time.Time
}

// spawn runs f in a goroutine unless too many operations of this type are already
// in flight, in which case it counts a drop and returns false.
func (r *runner) spawn(st *opStats, size int64, f func()) bool {
	sem := r.sems[st.name]
	select {
	case sem <- struct{}{}:
	default:
		st.drop(size)
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
	r.store.setState(n, stWriting)
	key := fmt.Sprintf("%s%06d/%03d", r.prefix, n/shardSize, n%shardSize)
	data := r.pool[off : off+int(size)]
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.timeout)
	defer cancel()
	if err := r.s3.put(ctx, key, data); err != nil {
		r.store.setState(n, stUnknown) // the upload may or may not have landed
		return time.Since(intended), nil, err
	}
	lat := time.Since(intended)
	return lat, &obj{key: key, n: n, size: size, crc: crc32.Checksum(data, castagnoli)}, nil
}

func (r *runner) doWrite(intended time.Time, size int64, off int) {
	st := r.stats["write"]
	lat, o, err := r.putObject(intended, size, off)
	if err != nil {
		r.store.abort(size)
		st.record(outErr, lat, 0, size, err)
		return
	}
	st.record(outOK, lat, size, size, nil)
	r.store.commit(o)
}

func (r *runner) doRead(intended time.Time, o *obj) {
	st := r.stats["read"]
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.timeout)
	defer cancel()
	n, sum, err := r.s3.get(ctx, o.key, r.cfg.verify)
	lat := time.Since(intended)
	// Checked after the read, once the body has been consumed. The flag only ever goes
	// from false to true, so "still false" means no delete started before we finished,
	// and a mismatch really is corruption. If it is true, a delete may have overlapped
	// the read (a short body, a 404, ...) and nothing can be concluded: count a race.
	deleted := o.deleted.Load()

	var bad error
	switch {
	case err != nil && deleted:
		st.record(outRaced, lat, 0, o.size, nil)
		return
	case err != nil:
		st.record(outErr, lat, 0, o.size, err)
		return
	case n != o.size:
		bad = fmt.Errorf("size mismatch on %s: got %d bytes, wrote %d", o.key, n, o.size)
	case r.cfg.verify && sum != o.crc:
		bad = fmt.Errorf("content mismatch on %s (checksum %08x, wrote %08x)", o.key, sum, o.crc)
	}
	if bad != nil {
		if deleted {
			st.record(outRaced, lat, 0, o.size, nil)
			return
		}
		fmt.Fprintf(os.Stderr, "CORRUPTION: %v\n", bad)
		st.record(outCorrupt, lat, 0, o.size, bad)
		return
	}
	st.record(outOK, lat, n, o.size, nil)
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
		r.store.setState(o.n, stUnknown) // it may still exist
		st.record(outErr, lat, 0, o.size, err)
		return
	}
	r.store.setState(o.n, stDeleted)
	st.record(outOK, lat, 0, o.size, nil)
}

// pace calls fire on a fixed schedule of `rate` per second until ctx ends. The
// schedule never waits for fire's work to complete (open loop). If the pacer itself
// falls more than 5s behind (e.g. the machine was suspended) it skips ahead rather
// than firing a huge burst, and reports how many scheduled operations that skipped.
func pace(ctx context.Context, rate float64, fire func(intended time.Time), behind func(missed int64, lag time.Duration)) {
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
		if lag := time.Since(next); lag > 5*time.Second {
			behind(int64(lag/interval), lag)
			next = time.Now()
		}
	}
}

// startStreams launches one pacer per operation type with a non-zero rate.
func (r *runner) startStreams(ctx context.Context, seed int64, wg *sync.WaitGroup) {
	launch := func(name string, fire func(time.Time)) {
		st := r.stats[name]
		if st.target <= 0 {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			pace(ctx, st.target, fire, func(missed int64, lag time.Duration) {
				st.dropN(missed)
				fmt.Fprintf(os.Stderr, "WARNING: %s pacer fell behind by %s (machine suspended or overloaded?); "+
					"%d scheduled operations counted as dropped\n", name, lag.Round(time.Second), missed)
			})
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
		if !r.spawn(st, size, func() { r.doWrite(intended, size, off) }) {
			r.store.abort(size)
		}
	})
	launch("read", func(intended time.Time) {
		st := r.stats["read"]
		o := r.store.pick(r.cfg.readSkew)
		if o == nil {
			st.skip()
			return
		}
		r.spawn(st, o.size, func() { r.doRead(intended, o) })
	})
	launch("delete", func(intended time.Time) {
		st := r.stats["delete"]
		o := r.store.take()
		if o == nil {
			st.skip()
			return
		}
		if !r.spawn(st, o.size, func() { r.doDelete(intended, o) }) {
			r.store.putBack(o)
		}
	})
}

// prefill writes objects (untimed) until the live set holds about `target` bytes.
// It gives up, with an error, after prefillMaxConsecutiveFailures failures in a row.
func (r *runner) prefill(ctx context.Context, target int64, conc int, seed int64) error {
	rng := rand.New(rand.NewSource(seed + 2))
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed, consecutive atomic.Int64
	var lastErr atomic.Value
	lastPrint := time.Now()
	for ctx.Err() == nil && consecutive.Load() < prefillMaxConsecutiveFailures {
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
				consecutive.Add(1)
				if failed.Add(1) <= 3 {
					fmt.Fprintf(os.Stderr, "  prefill error: %v\n", err)
				}
				lastErr.Store(err.Error())
				r.store.abort(size)
				return
			}
			consecutive.Store(0)
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
	if consecutive.Load() >= prefillMaxConsecutiveFailures {
		msg, _ := lastErr.Load().(string)
		return fmt.Errorf("prefill gave up after %d consecutive write failures (is the server up?): %s",
			prefillMaxConsecutiveFailures, msg)
	}
	return nil
}

// ----------------------------------------------------------- listing checks --

// listLoop lists one directory of objects every `every` and compares it with what
// the test believes is stored.
func (r *runner) listLoop(ctx context.Context, every time.Duration, seed int64) {
	rng := rand.New(rand.NewSource(seed + 4))
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.listCheck(ctx, rng)
		}
	}
}

// listCheck lists a random shard and reports objects that should be there but are
// not, objects whose successful delete did not stick, and keys nobody wrote. Object
// states are sampled before and after the listing; only states that did not change
// are judged, so operations in flight cannot cause false alarms.
func (r *runner) listCheck(ctx context.Context, rng *rand.Rand) {
	seqNow := r.seq.Load()
	if seqNow == 0 {
		return
	}
	shard := rng.Int63n((seqNow-1)/shardSize + 1)
	lo := shard * shardSize
	shardPrefix := fmt.Sprintf("%s%06d/", r.prefix, shard)

	before := r.store.statesRange(lo, lo+shardSize)
	keys, err := r.admin.listKeys(ctx, shardPrefix)
	after := r.store.statesRange(lo, lo+shardSize)
	if err != nil {
		if ctx.Err() == nil {
			r.lc.errors.Add(1)
			fmt.Fprintf(os.Stderr, "list check of %s failed: %v\n", shardPrefix, err)
		}
		return
	}
	r.lc.checks.Add(1)

	var present [shardSize]bool
	var foreign []string
	for _, k := range keys {
		idx, err := strconv.Atoi(strings.TrimPrefix(k, shardPrefix))
		if err != nil || idx < 0 || idx >= shardSize {
			foreign = append(foreign, k)
			continue
		}
		present[idx] = true
	}
	var missing, undeleted []string
	for i := 0; i < shardSize; i++ {
		key := fmt.Sprintf("%s%03d", shardPrefix, i)
		switch {
		case before[i] == stLive && after[i] == stLive && !present[i]:
			missing = append(missing, key)
		case present[i] && before[i] == stDeleted && after[i] == stDeleted:
			undeleted = append(undeleted, key)
		case present[i] && before[i] == stNone && after[i] == stNone:
			foreign = append(foreign, key)
		}
	}
	r.lc.missing.Add(int64(len(missing)))
	r.lc.undeleted.Add(int64(len(undeleted)))
	r.lc.foreign.Add(int64(len(foreign)))
	example := func(keys []string) string {
		if len(keys) == 0 {
			return ""
		}
		return " e.g. " + keys[0]
	}
	if len(missing)+len(undeleted)+len(foreign) > 0 {
		fmt.Fprintf(os.Stderr, "LIST MISMATCH in %s (%d keys listed): %d live objects missing%s; "+
			"%d deleted objects still listed%s; %d unexpected keys%s\n",
			shardPrefix, len(keys), len(missing), example(missing), len(undeleted), example(undeleted),
			len(foreign), example(foreign))
	}
}

// ----------------------------------------------------------- server metrics --

type sample struct{ name, labels, value string }

// parseMetrics extracts the samples whose metric name starts with one of prefixes
// from Prometheus text exposition format. Histogram buckets are skipped.
func parseMetrics(rd io.Reader, prefixes []string) []sample {
	var out []sample
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		if cut < 0 {
			continue
		}
		series, value := line[:cut], line[cut+1:]
		name, labels := series, ""
		if i := strings.IndexByte(series, '{'); i >= 0 {
			name, labels = series[:i], strings.TrimSuffix(series[i+1:], "}")
		}
		if strings.HasSuffix(name, "_bucket") {
			continue
		}
		for _, p := range prefixes {
			if strings.HasPrefix(name, p) {
				out = append(out, sample{name: name, labels: labels, value: value})
				break
			}
		}
	}
	return out
}

func scrapeOnce(hc *http.Client, url string, prefixes []string) ([]sample, error) {
	resp, err := hc.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return parseMetrics(resp.Body, prefixes), nil
}

// scrapeLoop samples each metrics URL every `every` into out, in long format.
func scrapeLoop(ctx context.Context, urls, prefixes []string, every time.Duration, start time.Time, out *csvOut) {
	hc := &http.Client{Timeout: 5 * time.Second}
	warned := map[string]bool{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			out.flush()
			return
		case <-t.C:
		}
		now := time.Now()
		for _, u := range urls {
			samples, err := scrapeOnce(hc, u, prefixes)
			if err != nil {
				if !warned[u] {
					warned[u] = true
					fmt.Fprintf(os.Stderr, "scrape %s failed: %v (further failures for this URL are not repeated)\n", u, err)
				}
				continue
			}
			for _, s := range samples {
				out.write([]string{now.UTC().Format(time.RFC3339), strconv.FormatFloat(now.Sub(start).Seconds(), 'f', 2, 64),
					u, s.name, s.labels, s.value})
			}
		}
		out.flush()
	}
}

// ----------------------------------------------------------------- reporting -

func gib(b int64) float64 { return float64(b) / (1 << 30) }

// emit writes the CSV rows for the interval since `since`, prints a one-line
// status, and returns the time the interval ended.
func (r *runner) emit(since time.Time) time.Time {
	now := time.Now()
	dt := now.Sub(since).Seconds()
	if dt <= 0 {
		dt = 1e-9
	}
	objects, bytes := r.store.snapshot()
	elapsed := now.Sub(r.start).Seconds()
	ts := now.UTC().Format(time.RFC3339)
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	i := func(v int64) string { return strconv.FormatInt(v, 10) }

	line := fmt.Sprintf("%7.0fs live=%d/%.2fGiB", elapsed, objects, gib(bytes))
	for _, name := range opNames {
		st := r.stats[name]
		ro := st.roll()
		iv := ro.iv
		achieved := float64(iv.completed()) / dt
		r.out.write([]string{
			ts, f(elapsed), name, f(st.target), f(achieved),
			i(iv.OK), i(iv.Errors), i(iv.Raced), i(iv.Corrupt), i(iv.Dropped), i(iv.Skipped),
			f(iv.P50ms), f(iv.P95ms), f(iv.P99ms), f(iv.MaxMs), f(float64(iv.Bytes) / 1e6 / dt),
			i(int64(objects)), f(gib(bytes)), i(int64(len(r.sems[name]))),
		})
		for _, ss := range ro.bySize {
			t := ss.Totals
			if t.completed()+t.Dropped == 0 {
				continue
			}
			r.sizesOut.write([]string{ts, f(elapsed), name, i(ss.SizeBytes), i(t.completed()), i(t.Errors + t.Corrupt),
				i(t.Dropped), f(t.P50ms), f(t.P95ms), f(t.P99ms), f(t.MaxMs)})
		}
		line += fmt.Sprintf(" | %c %.1f/%.0f p95=%.0fms", name[0], achieved, st.target, iv.P95ms)
		if bad := iv.Errors + iv.Corrupt; bad > 0 {
			line += fmt.Sprintf(" err=%d", bad)
		}
		if iv.Dropped > 0 {
			line += fmt.Sprintf(" drop=%d", iv.Dropped)
			if st.credit == 0 {
				line += "(percentiles optimistic)"
			}
		}
		if iv.Skipped > 0 {
			line += fmt.Sprintf(" skip=%d", iv.Skipped)
		}
		for _, e := range ro.errs {
			fmt.Fprintf(os.Stderr, "  [%s] error: %s\n", name, e)
		}
	}
	r.out.flush() // flushed every interval so a killed run keeps its data
	r.sizesOut.flush()
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

type sizeReport = sizeSummary

type opReport struct {
	TargetOpsPerSec   float64      `json:"target_ops_per_sec"`
	AchievedOpsPerSec float64      `json:"achieved_ops_per_sec"`
	Totals            summary      `json:"totals"`
	BySize            []sizeReport `json:"by_size"`
}

type listReport struct {
	Checks    int64 `json:"checks"`
	Errors    int64 `json:"errors"`
	Missing   int64 `json:"missing"`   // live objects absent from a listing
	Undeleted int64 `json:"undeleted"` // deleted objects still listed
	Foreign   int64 `json:"foreign"`   // listed keys the test never wrote
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
	ListCheck   listReport          `json:"list_check"`
}

// growthNote says what the live set will do, based on what the prefill really wrote.
func growthNote(writeRate, deleteRate, mean float64, st *store, maxBytes int64, maxObjs, minLive int) string {
	objects, bytes := st.snapshot()
	netObjs := writeRate - deleteRate
	switch {
	case netObjs > 0:
		hoursBytes := math.Max(float64(maxBytes-bytes), 0) / (netObjs * mean) / 3600
		hoursObjs := math.Max(float64(maxObjs-objects), 0) / netObjs / 3600
		limit, hours := "-max-live-bytes ("+human(maxBytes)+")", hoursBytes
		if hoursObjs < hoursBytes {
			limit, hours = fmt.Sprintf("-max-live-objects (%d)", maxObjs), hoursObjs
		}
		return fmt.Sprintf("note: writes outpace deletes; the live set grows by %.2f MB/s and reaches %s in about %.1f h,\n"+
			"      after which writes are skipped. Raise -delete-rate to hold steady.", netObjs*mean/1e6, limit, hours)
	case netObjs < 0:
		return fmt.Sprintf("note: deletes outpace writes; the live set will drain to -min-live (%d objects).", minLive)
	}
	return "note: write and delete rates match; the live set should hold steady."
}

// ------------------------------------------------------------------- helpers -

// parseSize parses sizes like 4096, 8KiB, 1.5GiB, 64k, 2MB. All units are binary
// (1KB = 1KiB = 1024 bytes).
func parseSize(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	mult := 1.0
	for _, u := range []struct {
		suf string
		m   float64
	}{{"gib", 1 << 30}, {"mib", 1 << 20}, {"kib", 1 << 10}, {"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10},
		{"g", 1 << 30}, {"m", 1 << 20}, {"k", 1 << 10}, {"b", 1}} {
		if strings.HasSuffix(t, u.suf) {
			mult = u.m
			t = strings.TrimSuffix(t, u.suf)
			break
		}
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	n := v * mult
	if n < 1 || n >= math.MaxInt64 {
		return 0, fmt.Errorf("size %q is out of range", s)
	}
	return int64(n), nil
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
	creditDropped := flag.Bool("credit-dropped", true, "count each dropped operation as taking -timeout in the latency percentiles")
	startupWait := flag.Duration("startup-wait", 30*time.Second, "how long to keep retrying if the S3 gateway is unreachable at startup")
	timeout := flag.Duration("timeout", 60*time.Second, "per-operation timeout; timeouts count as errors and as latency")
	verify := flag.Bool("verify", true, "verify content checksums on read")
	readSkew := flag.Float64("read-skew", 0, "0 = reads uniform over live objects; above 1 = Zipf-like hot/cold reads (1.1 mild, 1.5 strong)")
	listEvery := flag.Duration("list-check-interval", 10*time.Minute, "how often to list a directory and compare with expected contents (0 disables)")
	scrapeURLs := flag.String("scrape-urls", "", "comma-separated SeaweedFS /metrics URLs to sample every -interval into <out>-server.csv")
	scrapeMetrics := flag.String("scrape-metrics",
		"SeaweedFS_volumeServer_vacuuming,SeaweedFS_volumeServer_total_disk_size,SeaweedFS_volumeServer_volumes,"+
			"SeaweedFS_volumeServer_read_only_volumes,SeaweedFS_volumeServer_max_volumes,process_resident_memory_bytes,go_goroutines",
		"comma-separated metric name prefixes to keep when scraping")
	seed := flag.Int64("seed", 1, "seed for payload bytes and size choices")

	interval := flag.Duration("interval", 30*time.Second, "how often to write a CSV row and a status line")
	label := flag.String("label", "", "free-form label stored in the results")
	out := flag.String("out", "", "output file base name (default perfsoak-<run-id>)")
	purgeFlag := flag.Bool("purge", false, "delete leftover perfsoak data from earlier runs and exit")
	purgeOlder := flag.Duration("purge-older-than", 24*time.Hour, "with -purge, only remove runs that started longer ago than this (0 = all runs, including any still running)")
	flag.Parse()

	// --- validate ---
	dist, err := parseDist(*sizesFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "-sizes:", err)
		return 2
	}
	if dist.max() > 1<<30 {
		fmt.Fprintln(os.Stderr, "-sizes: objects above 1GiB are not supported (one payload buffer is held in memory)")
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
	if *writeRate > 100000 || *readRate > 100000 || *deleteRate > 100000 {
		fmt.Fprintln(os.Stderr, "rates above 100000 ops/s are not supported")
		return 2
	}
	if *maxInflight < 1 || *interval <= 0 || *duration <= 0 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "-max-inflight, -interval, -duration and -timeout must be positive")
		return 2
	}
	if *readSkew < 0 || (*readSkew > 0 && *readSkew <= 1) {
		fmt.Fprintln(os.Stderr, "-read-skew must be 0 (uniform) or greater than 1")
		return 2
	}
	var urls []string
	for _, u := range strings.Split(*scrapeURLs, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	var prefixes []string
	for _, p := range strings.Split(*scrapeMetrics, ",") {
		if p = strings.TrimSpace(p); p != "" {
			prefixes = append(prefixes, p)
		}
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

	conns := *maxInflight*3 + 16
	workload := newS3(strings.TrimRight(*endpoint, "/"), *bucket, *access, *secret, *region, conns, false)
	admin := newS3(strings.TrimRight(*endpoint, "/"), *bucket, *access, *secret, *region, 16, true)

	if *purgeFlag {
		ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		return purge(ctx, admin, *purgeOlder)
	}

	// --- set up: make sure the gateway answers before creating any files ---
	if err := waitForBucket(sigCtx, admin, *endpoint, *startupWait); err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		return 1
	}
	runID := newRunID()
	base := *out
	if base == "" {
		base = "perfsoak-" + runID
	}
	mainOut, err := newCSV(base+".csv", []string{"time", "elapsed_s", "op", "target_ops_s", "achieved_ops_s", "ok", "errors",
		"raced", "corrupt", "dropped", "skipped", "p50_ms", "p95_ms", "p99_ms", "max_ms", "mb_s", "live_objects", "live_gib", "inflight"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create csv:", err)
		return 1
	}
	defer mainOut.close()
	sizesOut, err := newCSV(base+"-sizes.csv", []string{"time", "elapsed_s", "op", "size_bytes", "completed", "errors",
		"dropped", "p50_ms", "p95_ms", "p99_ms", "max_ms"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create sizes csv:", err)
		return 1
	}
	defer sizesOut.close()
	var scrapeOut *csvOut
	if len(urls) > 0 {
		if scrapeOut, err = newCSV(base+"-server.csv", []string{"time", "elapsed_s", "source", "metric", "labels", "value"}); err != nil {
			fmt.Fprintln(os.Stderr, "create server csv:", err)
			return 1
		}
		defer scrapeOut.close()
	}

	runPrefix := rootPrefix + runID + "/"
	defer func() {
		// A fresh context: the main one is already cancelled after Ctrl-C.
		ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		fmt.Fprintf(os.Stderr, "cleaning up %s...\n", runPrefix)
		if err := admin.removePrefix(ctx, runPrefix); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup failed: %v\n  leftovers are under %s; remove them with: perfsoak -purge\n", err, runPrefix)
		}
	}()

	pool := make([]byte, dist.max()+(1<<20))
	rand.New(rand.NewSource(*seed)).Read(pool)

	rates := map[string]float64{"write": *writeRate, "read": *readRate, "delete": *deleteRate}
	var credit time.Duration
	if *creditDropped {
		credit = *timeout
	}
	r := &runner{
		cfg:      config{interval: *interval, timeout: *timeout, verify: *verify, readSkew: *readSkew},
		s3:       workload,
		admin:    admin,
		dist:     dist,
		pool:     pool,
		prefix:   runPrefix,
		stats:    map[string]*opStats{},
		sems:     map[string]chan struct{}{},
		out:      mainOut,
		sizesOut: sizesOut,
		store: &store{
			rng: rand.New(rand.NewSource(*seed + 3)), maxBytes: maxBytes, maxObjs: *maxObjs, minLive: *minLive,
		},
	}
	for _, name := range opNames {
		r.stats[name] = newOpStats(name, rates[name], credit, dist.sizes)
		r.sems[name] = make(chan struct{}, *maxInflight)
	}

	// --- tell the user what to expect ---
	mean := dist.mean()
	fmt.Printf("run %s -> %s.csv, %s-sizes.csv, %s.json\n", runID, base, base, base)
	fmt.Printf("targets: write %.1f/s, read %.1f/s, delete %.1f/s; mean object %.1f KiB; duration %s\n",
		*writeRate, *readRate, *deleteRate, mean/1024, *duration)
	fmt.Printf("expected bandwidth: writes %.2f MB/s, reads %.2f MB/s\n", *writeRate*mean/1e6, *readRate*mean/1e6)

	// --- prefill, then soak ---
	fmt.Printf("prefilling %s...\n", human(prefillBytes))
	if err := r.prefill(sigCtx, prefillBytes, 16, *seed); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if sigCtx.Err() != nil {
		// Nothing was measured, so there is nothing to summarise.
		fmt.Println("interrupted before the soak started; no results written")
		mainOut.close()
		sizesOut.close()
		os.Remove(mainOut.name)
		os.Remove(sizesOut.name)
		if scrapeOut != nil {
			scrapeOut.close()
			os.Remove(scrapeOut.name)
		}
		return 130
	}
	fmt.Println(growthNote(*writeRate, *deleteRate, mean, r.store, maxBytes, *maxObjs, *minLive))

	r.start = time.Now()
	soakCtx, stopSoak := context.WithTimeout(sigCtx, *duration)
	defer stopSoak()
	fmt.Println("soak started")
	stop, reportDone := make(chan struct{}), make(chan struct{})
	go r.reportLoop(stop, reportDone)
	var bg sync.WaitGroup
	r.startStreams(soakCtx, *seed, &bg)
	if *listEvery > 0 {
		bg.Add(1)
		go func() {
			defer bg.Done()
			r.listLoop(soakCtx, *listEvery, *seed)
		}()
	}
	if scrapeOut != nil {
		bg.Add(1)
		go func() {
			defer bg.Done()
			scrapeLoop(soakCtx, urls, prefixes, *interval, r.start, scrapeOut)
		}()
	}
	bg.Wait() // returns when the duration ends or Ctrl-C
	soakEnd := time.Now()
	r.wg.Wait() // let in-flight operations finish; this is not part of the soak's duration
	close(stop)
	<-reportDone
	interrupted := sigCtx.Err() != nil
	soakSeconds := soakEnd.Sub(r.start).Seconds()

	// --- summary ---
	rep := finalReport{
		RunID: runID, Label: *label, Start: r.start.UTC().Format(time.RFC3339),
		End: soakEnd.UTC().Format(time.RFC3339), SoakSeconds: soakSeconds,
		Interrupted: interrupted, Flags: map[string]string{}, Ops: map[string]opReport{},
		ListCheck: listReport{
			Checks: r.lc.checks.Load(), Errors: r.lc.errors.Load(), Missing: r.lc.missing.Load(),
			Undeleted: r.lc.undeleted.Load(), Foreign: r.lc.foreign.Load(),
		},
	}
	flag.VisitAll(func(f *flag.Flag) { rep.Flags[f.Name] = f.Value.String() })
	status := 0
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nOP\tTARGET/s\tACHIEVED/s\tOK\tERR\tCORRUPT\tRACED\tDROPPED\tSKIPPED\tP50ms\tP95ms\tP99ms\tMAXms")
	for _, name := range opNames {
		st := r.stats[name]
		t, by := st.total()
		achieved := float64(t.completed()) / soakSeconds
		rep.Ops[name] = opReport{TargetOpsPerSec: st.target, AchievedOpsPerSec: achieved, Totals: t, BySize: by}
		fmt.Fprintf(tw, "%s\t%.1f\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\n",
			name, st.target, achieved, t.OK, t.Errors, t.Corrupt, t.Raced, t.Dropped, t.Skipped,
			t.P50ms, t.P95ms, t.P99ms, t.MaxMs)
		if t.Errors > 0 || t.Corrupt > 0 {
			status = 1
		}
	}
	tw.Flush()

	tw = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nOP\tSIZE\tCOMPLETED\tERR\tDROPPED\tP50ms\tP95ms\tP99ms\tMAXms")
	for _, name := range opNames {
		for _, ss := range rep.Ops[name].BySize {
			t := ss.Totals
			if t.completed()+t.Dropped == 0 {
				continue
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\n", name, human(ss.SizeBytes), t.completed(),
				t.Errors+t.Corrupt, t.Dropped, t.P50ms, t.P95ms, t.P99ms, t.MaxMs)
		}
	}
	tw.Flush()

	if soakSeconds >= 5 { // too short a soak says nothing about sustained rates
		for _, name := range opNames {
			st, t := r.stats[name], rep.Ops[name]
			if st.target > 0 && t.AchievedOpsPerSec < 0.95*st.target {
				fmt.Printf("WARNING: %s achieved %.1f/s, under 95%% of the %.1f/s target (%d dropped, %d skipped)\n",
					name, t.AchievedOpsPerSec, st.target, t.Totals.Dropped, t.Totals.Skipped)
			}
		}
	}
	for _, name := range opNames {
		if t := rep.Ops[name].Totals; t.Corrupt > 0 {
			fmt.Printf("CORRUPTION DETECTED: %d %s operations returned data that did not match what was written\n", t.Corrupt, name)
		}
		if t := rep.Ops[name].Totals; t.Dropped > 0 && credit == 0 {
			fmt.Printf("NOTE: %d %s operations were dropped and not credited: percentiles are optimistic for those intervals\n", t.Dropped, name)
		}
	}
	if lr := rep.ListCheck; lr.Checks+lr.Errors > 0 {
		fmt.Printf("list checks: %d done, %d failed; missing %d, undeleted %d, unexpected %d\n",
			lr.Checks, lr.Errors, lr.Missing, lr.Undeleted, lr.Foreign)
		if lr.Missing+lr.Undeleted+lr.Foreign > 0 {
			status = 1
		}
	}

	if b, err := json.MarshalIndent(rep, "", "  "); err != nil {
		fmt.Fprintln(os.Stderr, "encode summary:", err)
		status = 1
	} else if err := os.WriteFile(base+".json", b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write summary:", err)
		status = 1
	} else {
		fmt.Printf("\nwrote %s.csv, %s-sizes.csv and %s.json\n", base, base, base)
	}

	// Make sure every file is on disk, and learn about write failures, before we exit.
	mainOut.close()
	sizesOut.close()
	outputs := []*csvOut{mainOut, sizesOut}
	if scrapeOut != nil {
		scrapeOut.close()
		outputs = append(outputs, scrapeOut)
	}
	for _, o := range outputs {
		if o.err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s is incomplete: %v\n", o.name, o.err)
			status = 1
		}
	}

	if status == 0 && interrupted {
		return 130
	}
	return status
}
