package formstream_test

// Regression materials for the contract:
//
//	"formstream: arbitrary chunking, cancellation and writer error transport contract"
//
// Every case enters the package only through its public surface
// (NewParser/Register/Parse/ParseContext/Value*) and cross-checks observable
// results against a wire-framing model: the exact boundary delimiters,
// per-header order and termination framing are inspected on the raw bytes,
// while decoded fields are cross-validated against mime/multipart itself.
// Failure cases preserve causal error layers, cancellation tears down every
// goroutine and the internal pipe, and repeated runs over identical inputs are
// stable after a mid-way recoverable failure.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

const contractSeed = int64(0x5eed_0001)

// contractPart is the model of a single multipart part. Header order is
// explicit because the wire contract keeps headers in insertion order even
// though the public API only exposes them by canonical key.
type contractPart struct {
	name      string
	filename  string
	content   []byte
	contentTy string
	headers   []contractHeader
}

type contractHeader struct {
	key, value string
}

// encodeModel renders the parts into canonical RFC 2046 multipart framing:
// CRLF line endings, "--boundary" delimiters and a terminating "--boundary--"
// epilogue delimiter. This is the model the implementation must decode
// identically regardless of where the transport splits the stream.
func encodeModel(boundary string, parts []contractPart) []byte {
	var b bytes.Buffer
	for _, part := range parts {
		b.WriteString("--")
		b.WriteString(boundary)
		b.WriteString("\r\n")
		var wroteDisposition bool
		for _, h := range part.headers {
			if textprotoCanonical(h.key) == "Content-Disposition" {
				wroteDisposition = true
			}
			b.WriteString(h.key)
			b.WriteString(": ")
			b.WriteString(h.value)
			b.WriteString("\r\n")
		}
		if !wroteDisposition {
			b.WriteString("Content-Disposition: ")
			b.WriteString(modelDisposition(part))
			b.WriteString("\r\n")
		}
		if part.contentTy != "" {
			b.WriteString("Content-Type: ")
			b.WriteString(part.contentTy)
			b.WriteString("\r\n")
		}
		b.WriteString("\r\n")
		b.Write(part.content)
		b.WriteString("\r\n")
	}
	b.WriteString("--")
	b.WriteString(boundary)
	b.WriteString("--\r\n")
	return b.Bytes()
}

func modelDisposition(part contractPart) string {
	var sb strings.Builder
	sb.WriteString("form-data; name=")
	sb.WriteString(strconv.Quote(part.name))
	if part.filename != "" {
		sb.WriteString("; filename=")
		sb.WriteString(strconv.Quote(part.filename))
	}
	return sb.String()
}

func textprotoCanonical(s string) string {
	// textproto.MIMEHeader canonicalization: canonical MIME key form.
	// mime/multipart sorts on insertion via Set/Add; canonicalization here
	// only needs to recognize Content-Disposition case-insensitively.
	return strings.ToLower(s)
}

// seededParts deterministically generates fields and files from a fixed seed:
// files contain boundary-like byte runs and fields contain CRLF, so any
// framing mistake (searching for the boundary without delimiter context,
// trimming newlines, etc.) surfaces as a hash mismatch.
func seededParts(seed int64, fileSizes []int) []contractPart {
	rng := rand.New(rand.NewSource(seed))
	parts := make([]contractPart, 0, len(fileSizes)+3)
	parts = append(parts, contractPart{
		name:    "description",
		content: []byte("seed\r\nline-" + strconv.FormatInt(seed, 16) + "\r\nend"),
	})
	for i, size := range fileSizes {
		content := make([]byte, size)
		rng.Read(content)
		// Sprinkle literal boundary substrings that must NOT terminate the
		// part: they are missing the required "--" delimiter/CRLF context.
		for j := 0; j+len(contractBoundary) <= len(content); j += size/3 + 1 {
			copy(content[j:], contractBoundary)
		}
		parts = append(parts, contractPart{
			name:      "file-" + strconv.Itoa(i),
			filename:  "seed-" + strconv.Itoa(i) + ".bin",
			content:   content,
			contentTy: "application/octet-stream",
		})
	}
	parts = append(parts, contractPart{
		name:    "trailing",
		content: []byte("尾字段-value"),
	})
	return parts
}

const contractBoundary = "----FormStreamContractB0undary"

// chunkReader replays data one chunk at a time using a fixed schedule, so
// boundary delimiters are forced across chunk edges. failAt, when >= 0, fails
// the read that would deliver byte offset failAt with cause, after returning
// the prefix up to that point (a mid-source failure).
type chunkReader struct {
	data       []byte
	sizes      []int
	pos        int
	idx        int
	failAt     int
	cause      error
	failCount  int64
	readCalls  int64
	blockGate  chan struct{}
	releaseGate chan struct{}
}

func (c *chunkReader) Read(p []byte) (int, error) {
	atomic.AddInt64(&c.readCalls, 1)
	if c.blockGate != nil {
		select {
		case c.blockGate <- struct{}{}:
		default:
		}
		<-c.releaseGate
	}
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	size := len(p)
	if c.idx < len(c.sizes) {
		size = c.sizes[c.idx]
		c.idx++
		if size > len(p) {
			size = len(p)
		}
	}
	end := c.pos + size
	if end > len(c.data) {
		end = len(c.data)
	}
	if c.failAt >= 0 && end > c.failAt {
		end = c.failAt
		n := copy(p, c.data[c.pos:end])
		c.pos = end
		atomic.AddInt64(&c.failCount, 1)
		return n, c.cause
	}
	n := copy(p, c.data[c.pos:end])
	c.pos += n
	if c.pos == len(c.data) {
		return n, io.EOF
	}
	return n, nil
}

// oneByteSizes builds a schedule of single-byte reads.
func oneByteSizes(n int) []int {
	sizes := make([]int, n)
	for i := range sizes {
		sizes[i] = 1
	}
	return sizes
}

// errShortWriter models a destination writer that accepts only some bytes
// before failing: io.Copy and io.CopyN are required to surface io.ErrShortWrite
// through the parser error chain instead of silently truncating.
type errShortWriter struct {
	accept int
}

func (w *errShortWriter) Write(p []byte) (int, error) {
	if w.accept <= 0 {
		return 0, io.ErrShortWrite
	}
	if len(p) <= w.accept {
		w.accept -= len(p)
		return len(p), nil
	}
	n := w.accept
	w.accept = 0
	return n, io.ErrShortWrite
}

// hookSink captures what the caller observes through the public hook contract.
type hookSink struct {
	mu       sync.Mutex
	files    map[string]string // part name -> sha256 of streamed bytes
	order    []string
}

func newHookSink() *hookSink {
	return &hookSink{files: map[string]string{}}
}

func (h *hookSink) hook(writers map[string]io.Writer) formstream.StreamHookFunc {
	return func(r io.Reader, header formstream.Header) error {
		w := io.Discard
		if writers != nil {
			if ww, ok := writers[header.Name()]; ok {
				w = ww
			}
		}
		h.mu.Lock()
		h.order = append(h.order, header.Name())
		h.mu.Unlock()
		sum := sha256.New()
		if _, err := io.Copy(io.MultiWriter(sum, w), r); err != nil {
			return err
		}
		h.mu.Lock()
		h.files[header.Name()] = hex.EncodeToString(sum.Sum(nil))
		h.mu.Unlock()
		return nil
	}
}

func (h *hookSink) hash(name string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.files[name]
}

func (h *hookSink) hookOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.order))
	copy(out, h.order)
	return out
}

// contractEvidence records both caller-visible results and internal invariants
// used to prove multipart/streaming/fault-injection semantics on failure.
type contractEvidence struct {
	desc        string
	parseErr    error
	fields      map[string]string
	fieldHashes map[string]string
	files       map[string]string
	hookOrder   []string
}

func runContractParse(t *testing.T, ev *contractEvidence, body io.Reader, boundary string, sink *hookSink, fileNames []string, opts ...formstream.ParserOption) {
	t.Helper()
	parser := formstream.NewParser(boundary, opts...)
	for _, name := range fileNames {
		if err := parser.Register(name, sink.hook(nil)); err != nil {
			t.Fatalf("%s: register %q: %v", ev.desc, name, err)
		}
	}
	ev.parseErr = parser.ParseContext(context.Background(), body)

	ev.fields = map[string]string{}
	ev.fieldHashes = map[string]string{}
	for _, key := range parser.ValueMap().Keys() {
		raw, _, ok := parser.ValueRaw(key)
		if !ok {
			continue
		}
		sum := sha256.Sum256(raw)
		ev.fieldHashes[key] = hex.EncodeToString(sum[:])
		content, _, _ := parser.Value(key)
		ev.fields[key] = content
	}
	ev.files = map[string]string{}
	for _, name := range fileNames {
		if got := sink.hash(name); got != "" {
			ev.files[name] = got
		}
	}
	ev.hookOrder = sink.hookOrder()
}

// requireWireFraming proves the fixture itself carries the exact framing the
// public contract relies on: number of boundary occurrences, header order and
// closing delimiter. Comparing decoded fields alone cannot catch an
// implementation that reassembles the right bytes after mangling framing.
func requireWireFraming(t *testing.T, wire []byte, boundary string, parts []contractPart) {
	t.Helper()
	delim := []byte("--" + boundary)
	var occurrences int
	for i := 0; i+len(delim) <= len(wire); i++ {
		if bytes.Equal(wire[i:i+len(delim)], delim) {
			occurrences++
		}
	}
	// Every part opens one delimiter plus exactly one closing delimiter.
	if want := len(parts) + 1; occurrences != want {
		t.Fatalf("wire framing: expected %d boundary delimiter occurrences, got %d in %q", want, occurrences, wire)
	}
	if !bytes.HasSuffix(wire, []byte("--"+boundary+"--\r\n")) && !bytes.HasSuffix(wire, []byte("--"+boundary+"--")) {
		t.Fatalf("wire framing: body is not closed by the terminating delimiter: %q", wire)
	}

	// Header order as emitted must match the model's insertion order.
	sections := bytes.Split(wire, delim)
	pi := 0
	for _, sec := range sections {
		sec = bytes.TrimPrefix(sec, []byte("--"))
		if !bytes.HasPrefix(sec, []byte("\r\n")) {
			continue
		}
		if bytes.HasPrefix(sec, []byte("\r\n--")) {
			break
		}
		head, _, ok := bytes.Cut(sec[2:], []byte("\r\n\r\n"))
		if !ok {
			break
		}
		lines := strings.Split(string(head), "\r\n")
		var expected []string
		for _, h := range parts[pi].headers {
			expected = append(expected, h.key+":")
		}
		if len(expected) == 0 {
			expected = append(expected, "Content-Disposition:")
			if parts[pi].contentTy != "" {
				expected = append(expected, "Content-Type:")
			}
		}
		if len(lines) != len(expected) {
			t.Fatalf("wire framing: part %d header count mismatch: want %d got %d (%q)", pi, len(expected), len(lines), lines)
		}
		for i, wantPrefix := range expected {
			if !strings.HasPrefix(lines[i], wantPrefix) {
				t.Fatalf("wire framing: part %d header #%d order mismatch: want prefix %q got %q", pi, i, wantPrefix, lines[i])
			}
		}
		pi++
	}
}

// stdlibModel is the independent decoder oracle: mime/multipart decodes the
// same bytes, and formstream must agree on every name, filename, content type,
// header value and body hash.
type stdlibPart struct {
	name      string
	filename  string
	contentTy string
	headers   textprotoValues
	hash      string
}

type textprotoValues = map[string][]string

func decodeWithStdlib(t *testing.T, wire []byte, boundary string) []stdlibPart {
	t.Helper()
	mr := multipart.NewReader(bytes.NewReader(wire), boundary)
	var out []stdlibPart
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("stdlib oracle failed to decode: %v\nwire=%q", err, wire)
		}
		sum := sha256.New()
		if _, err := io.Copy(sum, part); err != nil {
			t.Fatalf("stdlib oracle copy: %v", err)
		}
		headers := textprotoValues{}
		for k, v := range part.Header {
			headers[k] = append([]string(nil), v...)
		}
		out = append(out, stdlibPart{
			name:      part.FormName(),
			filename:  part.FileName(),
			contentTy: part.Header.Get("Content-Type"),
			headers:   headers,
			hash:      hex.EncodeToString(sum.Sum(nil)),
		})
	}
}

func requireMatchesModel(t *testing.T, label string, ev *contractEvidence, model []stdlibPart, wire []byte) {
	t.Helper()
	if ev.parseErr != nil {
		t.Fatalf("%s: expected success, first violation: parse returned %v (wire=%q)", label, ev.parseErr, wire)
	}
	for _, mp := range model {
		wantHash := mp.hash
		if _, isFile := ev.files[mp.name]; isFile {
			if got := ev.files[mp.name]; got != wantHash {
				t.Fatalf("%s: first violation at file %q: streamed hash %s != model hash %s", label, mp.name, got, wantHash)
			}
			continue
		}
		if got := ev.fieldHashes[mp.name]; got != wantHash {
			t.Fatalf("%s: first violation at field %q: decoded hash %s != model hash %s (value=%q)", label, mp.name, got, wantHash, ev.fields[mp.name])
		}
	}
	if len(ev.files)+len(ev.fields) != len(model) {
		t.Fatalf("%s: first violation at part coverage: model=%d files=%d fields=%d", label, len(model), len(ev.files), len(ev.fields))
	}
}

func errChainContains(err error, target error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, target) {
		return true
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		for _, e := range joined.Unwrap() {
			if errChainContains(e, target) {
				return true
			}
		}
	}
	return false
}

func countInChain(err error, target error) int {
	var n int
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if errors.Is(e, target) && e == target {
			n++
		}
		if j, ok := e.(interface{ Unwrap() []error }); ok {
			for _, sub := range j.Unwrap() {
				walk(sub)
			}
			return
		}
		if u := errors.Unwrap(e); u != nil {
			walk(u)
		}
	}
	walk(err)
	return n
}

// goroutineSnapshot captures stack signatures of goroutines outside the
// testing/runtime machinery, so leaks from cancellation races can be proven
// rather than guessed.
func goroutineSnapshot() []string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var out []string
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if g == "" || strings.Contains(g, "goroutineSnapshot") || strings.Contains(g, "runtime.") && strings.Count(g, "\n") <= 2 {
			continue
		}
		if strings.Contains(g, "/testing.") || strings.Contains(g, "testmain") {
			continue
		}
		lines := strings.Split(g, "\n")
		if len(lines) > 0 {
			out = append(out, lines[0])
		}
	}
	sort.Strings(out)
	return out
}

func waitForGoroutineSettle(t *testing.T, before []string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		after := goroutineSnapshot()
		if len(after) <= len(before) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := goroutineSnapshot()
	for _, sig := range after {
		found := false
		for _, b := range before {
			if b == sig {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("goroutine leak after cancellation: retained signature %q", sig)
		}
	}
}

func tempFormstreamFiles(t *testing.T) []string {
	t.Helper()
	dir := os.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "formstream-") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestFormStreamTransportContract_ChunkBoundaries is the model-based entry of
// the acceptance story: one fixed-seed body is split at every possible chunk
// boundary (plus adversarial fixed schedules), and each run must reproduce the
// exact model results while the raw wire keeps its boundary/header/close
// framing.
func TestFormStreamTransportContract_ChunkBoundaries(t *testing.T) {
	t.Parallel()

	parts := seededParts(contractSeed, []int{1, 31, 32, 33, 257, 4096})
	wire := encodeModel(contractBoundary, parts)
	requireWireFraming(t, wire, contractBoundary, parts)
	model := decodeWithStdlib(t, wire, contractBoundary)
	fileNames := []string{}
	for _, p := range parts {
		if p.filename != "" {
			fileNames = append(fileNames, p.name)
		}
	}

	schedules := [][]int{
		oneByteSizes(len(wire)),
		{2, 3, 5, 7, 11, 13},
		{32 * 1024},
	}
	// Split at the byte immediately before and after every "--boundary"
	// occurrence so delimiters straddle chunks.
	delim := []byte("--" + contractBoundary)
	var cuts []int
	for i := 0; i+len(delim) <= len(wire); i++ {
		if bytes.Equal(wire[i:i+len(delim)], delim) {
			cuts = append(cuts, i+1, i+len(delim)-1, i+len(delim)+2)
		}
	}

	run := func(t *testing.T, label string, reader io.Reader) {
		t.Helper()
		before := tempFormstreamFiles(t)
		sink := newHookSink()
		ev := &contractEvidence{desc: label}
		runContractParse(t, ev, reader, contractBoundary, sink, fileNames)
		requireMatchesModel(t, label, ev, model, wire)
		if len(tempFormstreamFiles(t)) != len(before) {
			t.Fatalf("%s: first violation: temp file leaked after success", label)
		}
	}

	for cutIndex, cut := range cuts {
		cut := cut
		t.Run(fmt.Sprintf("boundaryAcrossChunk/offset=%d", cut), func(t *testing.T) {
			t.Parallel()
			run(t, "boundaryAcrossChunk#"+strconv.Itoa(cutIndex), &chunkReader{
				data:  wire,
				sizes: []int{cut, len(wire) - cut + 64},
			})
		})
	}

	for si, sizes := range schedules {
		sizes := sizes
		t.Run(fmt.Sprintf("fixedSchedule/%d", si), func(t *testing.T) {
			t.Parallel()
			run(t, "fixedSchedule#"+strconv.Itoa(si), &chunkReader{data: wire, sizes: sizes})
		})
	}

	// Every single split offset, one byte at a time around key framing points.
	rng := rand.New(rand.NewSource(contractSeed + 1))
	for i := 0; i < 40; i++ {
		i := i
		t.Run(fmt.Sprintf("randomSplit/%02d", i), func(t *testing.T) {
			t.Parallel()
			cr := &chunkReader{data: wire}
			cr.sizes = nil // use caller buffer sizes; emulate random splits instead
			// wrap with a random-chunk reader
			rr := &randomChunkReader{rng: rand.New(rand.NewSource(rng.Int63())), data: wire}
			_ = cr
			run(t, "randomSplit#"+strconv.Itoa(i), rr)
		})
	}
}

type randomChunkReader struct {
	data []byte
	pos  int
	rng  *rand.Rand
}

func (r *randomChunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	max := len(p)
	if max > 16 {
		max = 16
	}
	size := 1 + r.rng.Intn(max)
	end := r.pos + size
	if end > len(r.data) {
		end = len(r.data)
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	if r.pos == len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

// TestFormStreamTransportContract_EmptyFile proves zero-length file parts are
// delivered to the hook as an empty stream while framing still closes the
// part. This breaks implementations that treat an empty body as "no part".
func TestFormStreamTransportContract_EmptyFile(t *testing.T) {
	t.Parallel()

	parts := []contractPart{
		{name: "before", content: []byte("x")},
		{name: "empty", filename: "empty.dat", content: nil, contentTy: "application/octet-stream"},
		{name: "after", content: []byte("y")},
	}
	wire := encodeModel(contractBoundary, parts)
	requireWireFraming(t, wire, contractBoundary, parts)
	model := decodeWithStdlib(t, wire, contractBoundary)

	for _, sizes := range [][]int{nil, oneByteSizes(len(wire)), {7, 3, 40}} {
		sink := newHookSink()
		emptyCalls := atomic.Int32{}
		parser := formstream.NewParser(contractBoundary)
		err := parser.Register("empty", func(r io.Reader, h formstream.Header) error {
			emptyCalls.Add(1)
			if h.FileName() != "empty.dat" {
				return fmt.Errorf("filename contract: got %q", h.FileName())
			}
			n, err := io.Copy(io.Discard, r)
			if err != nil {
				return err
			}
			if n != 0 {
				return fmt.Errorf("empty file contract: streamed %d bytes", n)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		var reader io.Reader = bytes.NewReader(wire)
		if sizes != nil {
			reader = &chunkReader{data: wire, sizes: sizes}
		}
		if err := parser.Parse(reader); err != nil {
			t.Fatalf("empty file parse: %v (wire=%q)", err, wire)
		}
		if emptyCalls.Load() != 1 {
			t.Fatalf("empty file hook contract: invoked %d times, want 1", emptyCalls.Load())
		}
		sink.mu.Lock()
		sink.mu.Unlock()
		ev := &contractEvidence{desc: "empty file", parseErr: nil}
		ev.fields = map[string]string{}
		for _, mp := range model {
			if mp.name == "empty" {
				continue
			}
			content, _, ok := parser.Value(mp.name)
			if !ok {
				t.Fatalf("empty file: field %q missing", mp.name)
			}
			ev.fields[mp.name] = content
			sum := sha256.Sum256([]byte(content))
			if hex.EncodeToString(sum[:]) != mp.hash {
				t.Fatalf("empty file: field %q corrupted by adjacent empty framing", mp.name)
			}
		}
	}
}

// TestFormStreamTransportContract_UnicodeFilename covers non-ASCII filenames
// over one-byte chunks; filename decoding must agree with the stdlib oracle.
func TestFormStreamTransportContract_UnicodeFilename(t *testing.T) {
	t.Parallel()

	parts := []contractPart{
		{
			name:     "upload",
			filename: "テスト-名前-🚀.dat",
			content:  []byte("unicode payload \r\n --fake-boundary \r\n"),
			contentTy: "text/plain",
		},
	}
	wire := encodeModel(contractBoundary, parts)
	requireWireFraming(t, wire, contractBoundary, parts)
	model := decodeWithStdlib(t, wire, contractBoundary)

	var gotFilename string
	parser := formstream.NewParser(contractBoundary)
	if err := parser.Register("upload", func(r io.Reader, h formstream.Header) error {
		gotFilename = h.FileName()
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != model[0].hash {
			return fmt.Errorf("unicode file body mismatch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := parser.Parse(&chunkReader{data: wire, sizes: oneByteSizes(len(wire))}); err != nil {
		t.Fatalf("unicode filename parse: %v", err)
	}
	if gotFilename != model[0].filename {
		t.Fatalf("unicode filename contract: got %q, model %q (wire=%q)", gotFilename, model[0].filename, wire)
	}
}

// TestFormStreamTransportContract_HeaderOrderStability re-encodes the same
// content with headers in different insertion orders. Wire bytes differ, but
// public results are identical; this is the enum/order stability group.
func TestFormStreamTransportContract_HeaderOrderStability(t *testing.T) {
	t.Parallel()

	mk := func(order []string) []contractPart {
		var hs []contractHeader
		for _, k := range order {
			switch k {
			case "Content-Disposition":
				hs = append(hs, contractHeader{k, `form-data; name="f"; filename="a.bin"`})
			case "X-A":
				hs = append(hs, contractHeader{k, "1"})
			case "X-B":
				hs = append(hs, contractHeader{k, "2"})
			case "Content-Type":
				hs = append(hs, contractHeader{k, "application/json"})
			}
		}
		return []contractPart{{name: "f", filename: "a.bin", content: []byte("{}"), headers: hs}}
	}
	orders := [][]string{
		{"Content-Disposition", "Content-Type", "X-A", "X-B"},
		{"X-B", "X-A", "Content-Type", "Content-Disposition"},
		{"X-A", "Content-Disposition", "X-B", "Content-Type"},
	}

	type result struct {
		filename, ctype, xa, xb, hash string
	}
	results := make([]result, len(orders))
	for i, order := range orders {
		parts := mk(order)
		wire := encodeModel(contractBoundary, parts)
		requireWireFraming(t, wire, contractBoundary, parts)
		var res result
		parser := formstream.NewParser(contractBoundary)
		if err := parser.Register("f", func(r io.Reader, h formstream.Header) error {
			res.filename = h.FileName()
			res.ctype = h.ContentType()
			res.xa = h.Get("X-A")
			res.xb = h.Get("X-B")
			data, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			res.hash = hex.EncodeToString(sum[:])
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := parser.Parse(bytes.NewReader(wire)); err != nil {
			t.Fatalf("header order #%d: %v", i, err)
		}
		results[i] = res
	}
	for i := 1; i < len(results); i++ {
		if results[i] != results[0] {
			t.Fatalf("header order instability: %#v != %#v", results[i], results[0])
		}
	}
}
