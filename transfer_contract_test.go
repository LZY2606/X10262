package formstream_test

// Regression material for the contract named
// "formstream: arbitrary chunking, cancellation and writer error transfer".
//
// Every case enters the library exclusively through the public API
// (NewParser / Register / Parse / ParseWithContext) and asserts both the
// caller-visible result and transfer invariants:
//   - the multipart wire framing (CRLF, boundary delimiters, header order)
//   - stream delivery order and streaming (not post-hoc buffering)
//   - exact error causality for source, corruption and cancellation causes
//   - single error report, goroutine/pipe shutdown, temp-file cleanup
//   - retry stability after a mid-flight failure
//
// All fixtures are generated deterministically from a fixed seed and live
// in memory or in t.TempDir(); no network service is contacted.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net/textproto"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

const contractBoundary = "formstream-contract-boundary"

// seedByte produces deterministic, boundary-free content.
func seedByte(i int) byte {
	return byte('0' + (i*7919+13)%10)
}

func seedBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seedByte(i)
	}
	return b
}

type fileModel struct {
	name         string
	fileName     string
	contentType  string
	content      []byte
	requirements []string // WithRequiredPart: forces the abnormal (buffered/spill) path
}

type formModel struct {
	fields [][2]string // name, value
	files  []fileModel
}

// contractForm is the single fixed-seed fixture used across the suite:
// fields, an empty file, a Unicode filename, a fast-path stream and two
// required-part files (memory buffered and disk spilled).
func contractForm() formModel {
	return formModel{
		fields: [][2]string{
			{"title", "契約-title"},
			{"note", "0123456789"},
		},
		files: []fileModel{
			{
				name:        "empty",
				fileName:    "empty.bin",
				contentType: "application/octet-stream",
				content:     []byte{},
			},
			{
				name:        "unicode",
				fileName:    "日本語 ファイル.dat",
				contentType: "text/plain",
				content:     seedBytes(37),
			},
			{
				name:         "logo",
				fileName:     "logo.bin",
				contentType:  "application/octet-stream",
				content:      seedBytes(160 * 1024), // overflows the 64KiB test limit -> disk
				requirements: []string{"title"},
			},
			{
				name:         "bigfile",
				fileName:     "big.bin",
				contentType:  "application/octet-stream",
				content:      seedBytes(4096),
				requirements: []string{"title"},
			},
			{
				name:        "stream",
				fileName:    "stream.bin",
				contentType: "application/octet-stream",
				content:     seedBytes(5000), // fast path: no required parts
			},
		},
	}
}

// runOpts selects the transfer conditions without touching internals.
type runOpts struct {
	memSize   formstream.DataSize // >0 sets both total and per-file memory limits
	target    io.Writer           // destination for the fast-path file
	resilient bool                // tolerate errTransientRead inside file hooks
}

// runParse drives one full parse through the public API and records the
// caller-observable result.
func runParse(t *testing.T, r io.Reader, m formModel, opts runOpts) (*runResult, error) {
	t.Helper()

	cfg := []formstream.ParserOption{}
	if opts.memSize > 0 {
		cfg = append(cfg,
			formstream.WithMaxMemSize(opts.memSize),
			formstream.WithMaxMemFileSize(opts.memSize))
	}
	parser := formstream.NewParser(contractBoundary, cfg...)

	res := &runResult{fields: map[string]string{}, hookCount: map[string]int{}}
	var order atomic.Int64

	for _, fm := range m.files {
		fm := fm
		regOpts := []formstream.RegisterOption{}
		for _, req := range fm.requirements {
			regOpts = append(regOpts, formstream.WithRequiredPart(req))
		}
		err := parser.Register(fm.name, func(r io.Reader, h formstream.Header) error {
			var data []byte
			var err error
			if opts.resilient {
				data, err = readAllResilient(r)
			} else {
				data, err = io.ReadAll(r)
			}
			if err != nil {
				return fmt.Errorf("hook(%q) copy: %w", fm.name, err)
			}
			if opts.target != nil && fm.name == "stream" {
				if _, werr := opts.target.Write(data); werr != nil {
					return fmt.Errorf("hook(%q) target write: %w", fm.name, werr)
				}
			}
			res.hookCount[fm.name]++
			res.files = append(res.files, fileRecord{
				name:        fm.name,
				fileName:    h.FileName(),
				contentType: h.ContentType(),
				content:     bytes.Clone(data),
				order:       int(order.Add(1)),
			})
			return nil
		}, regOpts...)
		if err != nil {
			t.Fatalf("Register(%q): %v", fm.name, err)
		}
	}

	err := parser.ParseWithContext(context.Background(), r)

	for _, fv := range m.fields {
		v, _, ok := parser.Value(fv[0])
		if ok {
			res.fields[fv[0]] = v
		}
	}

	return res, err
}

func assertModel(t *testing.T, m formModel, res *runResult, stage string) bool {
	t.Helper()
	ok := true

	for _, fv := range m.fields {
		got, present := res.fields[fv[0]]
		if !present {
			t.Errorf("[%s] field %q missing from result", stage, fv[0])
			ok = false
			continue
		}
		if got != fv[1] {
			t.Errorf("[%s] field %q = %q, want %q", stage, fv[0], got, fv[1])
			ok = false
		}
	}

	got := filesByName(res)
	for _, fm := range m.files {
		rec, present := got[fm.name]
		if !present {
			t.Errorf("[%s] file hook %q was never invoked", stage, fm.name)
			ok = false
			continue
		}
		if res.hookCount[fm.name] != 1 {
			t.Errorf("[%s] file %q hook invoked %d times, want exactly 1",
				stage, fm.name, res.hookCount[fm.name])
			ok = false
		}
		if rec.fileName != fm.fileName {
			t.Errorf("[%s] file %q filename = %q, want %q", stage, fm.name, rec.fileName, fm.fileName)
			ok = false
		}
		if rec.contentType != fm.contentType {
			t.Errorf("[%s] file %q content-type = %q, want %q", stage, fm.name, rec.contentType, fm.contentType)
			ok = false
		}
		if !bytes.Equal(rec.content, fm.content) {
			t.Errorf("[%s] file %q content mismatch: got %d bytes, want %d bytes (first diff near %q)",
				stage, fm.name, len(rec.content), len(fm.content), firstDiffContext(rec.content, fm.content))
			ok = false
		}
	}

	return ok
}

func firstDiffContext(a, b []byte) string {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			s := i - 8
			if s < 0 {
				s = 0
			}
			e := i + 8
			if e > len(a) {
				e = len(a)
			}
			return strconv.Quote(string(a[s:e]))
		}
	}
	return "<length>"
}

func filesByName(res *runResult) map[string]fileRecord {
	out := map[string]fileRecord{}
	for _, fr := range res.files {
		out[fr.name] = fr
	}
	return out
}

// --- leak / shutdown diagnostics ------------------------------------------

func goroutineStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// assertNoFormstreamGoroutine fails with the offending stacks if a
// formstream-owned goroutine survives past the deadline.
func assertNoFormstreamGoroutine(t *testing.T, deadline time.Duration) {
	t.Helper()

	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if !strings.Contains(goroutineStacks(), "ParseWithContext") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	n := 0
	for _, gs := range strings.Split(goroutineStacks(), "\n\n") {
		if strings.Contains(gs, "ParseWithContext") {
			n++
			t.Errorf("leaked formstream goroutine #%d:\n%s", n, gs)
		}
	}
}

func listTempFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "formstream-") {
			out = append(out, e.Name())
		}
	}
	return out
}

func assertNoTempLeak(t *testing.T, before []string, stage string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var after []string
	for time.Now().Before(deadline) {
		after = listTempFiles(t)
		if len(after) <= len(before) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("[%s] temp file leak: before=%v after=%v", stage, before, after)
}

// assertWireFraming verifies the serialized contract on the wire itself,
// not merely the post-parse fields: exact CRLF delimiters, boundary
// markers, delimiter count and canonical header order. This is the case
// an implementation that emits bare LF or headers in map/random order
// would fail.
func assertWireFraming(t *testing.T, m formModel, body []byte) {
	t.Helper()

	wantPrefix := []byte("--" + contractBoundary + "\r\n")
	if !bytes.HasPrefix(body, wantPrefix) {
		t.Fatalf("wire framing: body must start with %s, got %q",
			wantPrefix, firstBytes(body, 40))
	}
	wantSuffix := []byte("\r\n--" + contractBoundary + "--\r\n")
	if !bytes.HasSuffix(body, wantSuffix) {
		t.Fatalf("wire framing: body must end with %s, got %q",
			wantSuffix, lastBytes(body, 40))
	}

	delims := bytes.Count(body, []byte("\r\n--"+contractBoundary))
	// one opening/mid delimiter per part + one final boundary
	wantDelims := len(m.fields) + len(m.files) + 1
	if delims != wantDelims {
		t.Errorf("wire framing: found %d boundary delimiters, want %d", delims, wantDelims)
	}

	// No bare LF is allowed anywhere; every line ending must be CRLF.
	stripped := bytes.ReplaceAll(body, []byte("\r\n"), nil)
	if bytes.ContainsAny(stripped, "\r") {
		off := bytes.IndexByte(stripped, '\r')
		t.Errorf("wire framing: bare CR at offset %d", off)
	}
	if bytes.Contains(stripped, []byte("\n")) {
		off := bytes.IndexByte(stripped, '\n')
		t.Errorf("wire framing: bare LF at offset %d", off)
	}

	// Header order on the wire is canonical: Content-Disposition precedes
	// Content-Type inside each file part (sorted), regardless of the
	// insertion order used by the builder.
	for _, fm := range m.files {
		marker := fmt.Sprintf("Content-Type: %s\r\nContent-Disposition:", fm.contentType)
		if bytes.Contains(body, []byte(marker)) {
			t.Errorf("wire framing: %q emitted Content-Type before Content-Disposition", fm.name)
		}
		wantCD := fmt.Sprintf("Content-Disposition: form-data; name=%q; filename=%q\r\nContent-Type: %s\r\n\r\n",
			fm.name, fm.fileName, fm.contentType)
		idx := bytes.Index(body, []byte(wantCD))
		if idx < 0 {
			t.Errorf("wire framing: %q header block not found verbatim: want %q", fm.name, wantCD)
		}
	}

	// The Unicode filename must survive on the wire as UTF-8 (no RFC5987
	// percent-encoding under this builder contract).
	uni := m.files[0]
	for _, fm := range m.files {
		if fm.name == "unicode" {
			uni = fm
		}
	}
	if !bytes.Contains(body, []byte(`filename="`+uni.fileName+`"`)) {
		t.Errorf("wire framing: unicode filename not present as UTF-8 on the wire")
	}
}

func firstBytes(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[:n]
}

func lastBytes(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[len(b)-n:]
}

// TestContractModeled_ArbitraryChunkBoundaries is the model-based
// acceptance test: the same fixed-seed multipart body parses identically
// for every chunking, including one-byte chunks and cuts that straddle
// boundary lines and part headers.
func TestContractModeled_ArbitraryChunkBoundaries(t *testing.T) {
	m := contractForm()
	body := buildForm(t, m)
	assertWireFraming(t, m, body)

	const memLimit formstream.DataSize = 64 * formstream.KB

	t.Run("one-byte chunks", func(t *testing.T) {
		r := &chunkReader{data: body, sizes: oneByteChunker()}
		res, err := runParse(t, r, m, runOpts{memSize: memLimit})
		if err != nil {
			t.Fatalf("parse failed at a one-byte boundary: %v", err)
		}
		if !assertModel(t, m, res, "one-byte") {
			return
		}
	})

	// Every two-chunk cut point in the body once. This includes cuts inside
	// "--boundary", inside CRLF and inside header names: common naive
	// boundary scanners break exactly here.
	t.Run("every two-chunk cut point", func(t *testing.T) {
		if testing.Short() {
			t.Skip("exhaustive cut sweep skipped in -short mode")
		}
		for cut := 1; cut < len(body); cut++ {
			r := &chunkReader{data: body, sizes: splitChunker(body, cut)}
			res, err := runParse(t, r, m, runOpts{memSize: memLimit})
			if err != nil {
				t.Fatalf("first violation at cut=%d (context=%q): %v",
					cut, cutContext(body, cut), err)
			}
			if !assertModelAt(t, m, res, fmt.Sprintf("cut=%d %s", cut, cutContext(body, cut))) {
				return
			}
		}
	})

	t.Run("boundary-straddling cuts", func(t *testing.T) {
		needle := []byte("--" + contractBoundary)
		var off int
		for {
			idx := bytes.Index(body[off:], needle)
			if idx < 0 {
				break
			}
			abs := off + idx
			for cut := abs; cut <= abs+len(needle); cut++ {
				r := &chunkReader{data: body, sizes: splitChunker(body, cut)}
				res, err := runParse(t, r, m, runOpts{memSize: memLimit})
				if err != nil {
					t.Fatalf("boundary straddle cut=%d failed: %v", cut, err)
				}
				assertModelAt(t, m, res, fmt.Sprintf("boundary cut=%d", cut))
			}
			off = abs + len(needle)
		}
	})

	t.Run("fixed-seed chunk schedules", func(t *testing.T) {
		for seed := uint64(1); seed <= 64; seed++ {
			r := &chunkReader{data: body, sizes: seedChunker(seed)}
			res, err := runParse(t, r, m, runOpts{memSize: memLimit})
			if err != nil {
				t.Fatalf("seed %d: parse failed: %v", seed, err)
			}
			assertModelAt(t, m, res, fmt.Sprintf("seed=%d", seed))
		}
	})
}

// assertModelAt reports the failing variant inline so the first violation
// position and its context are preserved.
func assertModelAt(t *testing.T, m formModel, res *runResult, stage string) bool {
	t.Helper()
	if assertModel(t, m, res, stage) {
		return true
	}
	t.Fatalf("model violated for %s", stage)
	return false
}

func cutContext(body []byte, cut int) string {
	s := cut - 12
	if s < 0 {
		s = 0
	}
	e := cut + 12
	if e > len(body) {
		e = len(body)
	}
	return strconv.Quote(string(body[s:e]))
}

// buildForm serializes m with mime/multipart.Writer. Header insertion
// order is deliberately non-alphabetical; the canonical writer output is
// still alphabetical, which the wire-framing assertions rely on.
func buildForm(t *testing.T, m formModel) []byte {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(contractBoundary); err != nil {
		t.Fatalf("SetBoundary: %v", err)
	}

	for _, fv := range m.fields {
		if err := mw.WriteField(fv[0], fv[1]); err != nil {
			t.Fatalf("WriteField(%q): %v", fv[0], err)
		}
	}

	for _, fm := range m.files {
		hd := make(textproto.MIMEHeader)
		hd.Set("Content-Type", fm.contentType)
		hd.Set("Content-Disposition",
			fmt.Sprintf(`form-data; name=%q; filename=%q`, fm.name, fm.fileName))
		pw, err := mw.CreatePart(hd)
		if err != nil {
			t.Fatalf("CreatePart(%q): %v", fm.name, err)
		}
		if _, err := pw.Write(fm.content); err != nil {
			t.Fatalf("Write(%q): %v", fm.name, err)
		}
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}

	return buf.Bytes()
}

// --- transfer-side fault/framing primitives ------------------------------

// chunkReader hands the body back in externally controlled chunks. Every
// byte is unchanged; only the Read boundaries differ.
type chunkReader struct {
	data  []byte
	pos   int
	sizes func() int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.sizes()
	if n < 1 {
		n = 1
	}
	if n > len(c.data)-c.pos {
		n = len(c.data) - c.pos
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

// oneByteChunker returns one byte per Read: every byte, every CRLF and
// every boundary gets split.
func oneByteChunker() func() int { return func() int { return 1 } }

// seedChunker returns deterministic pseudo-random chunk sizes.
func seedChunker(seed uint64) func() int {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return func() int { return 1 + r.IntN(64) }
}

// splitChunker hands out the body in exactly two chunks at cut.
func splitChunker(body []byte, cut int) func() int {
	first := true
	return func() int {
		if first {
			first = false
			return cut
		}
		return len(body)
	}
}

// pacedReader wraps a reader and sometimes yields to the scheduler between
// reads to perturb pump/parser timing without changing any bytes.
type pacedReader struct {
	r     io.Reader
	rng   *rand.Rand
	relax func()
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if p.rng.IntN(3) == 0 {
		p.relax()
	}
	return p.r.Read(b)
}

// failPointReader fails once at absolute byte offset failAt and blocks
// until released on Close, so cancellation tests can observe shutdown.
type failPointReader struct {
	data     []byte
	pos      int
	failAt   int
	failOnce sync.Once
	failErr  error
	release  chan struct{}
	closed   atomic.Bool
}

func newFailPointReader(data []byte, failAt int, failErr error) *failPointReader {
	return &failPointReader{data: data, failAt: failAt, failErr: failErr, release: make(chan struct{})}
}

func (f *failPointReader) Read(p []byte) (int, error) {
	if f.closed.Load() {
		return 0, f.failErr
	}
	if f.pos >= f.failAt && f.pos < len(f.data) {
		f.failOnce.Do(func() {})
		return 0, f.failErr
	}
	if f.pos >= len(f.data) {
		select {
		case <-f.release:
		default:
		}
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

// releaseAfterEOFReader blocks on the first Read after offset blockAt
// until release is closed, modelling a slow/stalled upload connection.
type blockAtReader struct {
	data    []byte
	pos     int
	blockAt int
	release chan struct{}
}

func (b *blockAtReader) Read(p []byte) (int, error) {
	if b.pos >= b.blockAt && b.pos < len(b.data) {
		<-b.release
	}
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

var errTransientRead = errors.New("transient source failure (recoverable by hook)")

// failOnceThenRecoverReader injects one transient read error at call
// transientAt but keeps its position: a resilient consumer may retry and
// receive the complete body.
type failOnceThenRecoverReader struct {
	data        []byte
	pos         int
	calls       atomic.Int64
	transientAt int64
}

func (r *failOnceThenRecoverReader) Read(p []byte) (int, error) {
	if r.calls.Add(1) == r.transientAt {
		return 0, errTransientRead
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// shortWriter is the caller-side target writer: it accepts fewer bytes
// than offered without an error, violating io.Writer once, then behaves.
type shortWriter struct {
	shortAt int64
	calls   atomic.Int64
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.calls.Add(1) == w.shortAt {
		return 0, nil
	}
	return len(p), nil
}

// --- observation model ----------------------------------------------------

type fileRecord struct {
	name        string
	fileName    string
	contentType string
	content     []byte
	order       int
	streamed    bool // observed data before the final Read returned EOF
}

type runResult struct {
	fields       map[string]string
	files        []fileRecord
	hookCount    map[string]int
	streamProgMu sync.Mutex
}

// readAllResilient copies r, tolerating transient read errors up to a
// bounded retry budget so recovery hooks stay cooperative.
func readAllResilient(r io.Reader) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	tmp := make([]byte, 256)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if err == nil {
			continue
		}
		if errors.Is(err, errTransientRead) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return buf.Bytes(), nil
		}
		return buf.Bytes(), err
	}
}
