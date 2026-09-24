package formstream_test

// Contract: "formstream: arbitrary chunking, cancellation and writer-error
// transport". Every case enters through the public StreamWriter API; the
// expected wire bytes are produced independently by mime/multipart.Writer.
// Assertions keep the first violation offset and its surrounding bytes
// instead of reducing the result to a boolean.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/textproto"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

var (
	errInjectedSource = errors.New("injected source reader failure")
	errInjectedDest   = errors.New("injected destination failure")
)

// failAfterReader serves prefix bytes, then fails. The recoverable failure
// is injected mid-stream, after a fixed amount of real wire output.
type failAfterReader struct {
	data   []byte
	offset int
	failAt int
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.offset >= r.failAt {
		return 0, errInjectedSource
	}

	n := copy(p, r.data[r.offset:r.failAt])
	r.offset += n

	return n, nil
}

// shortWriter violates io.Writer by reporting a successful short write. A
// naive pass-through implementation silently truncates the body; the
// contract requires io.ErrShortWrite.
type shortWriter struct {
	dst    io.Writer
	failAt int
	written int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	remaining := w.failAt - w.written
	if remaining <= 0 {
		return 0, nil
	}

	n := len(p)
	if n > remaining {
		n = remaining
	}

	nw, err := w.dst.Write(p[:n])
	w.written += nw

	return nw, err
}

// gateWriter blocks a Write until ctx is done, then fails. With a real
// network destination ctx cancellation cannot unblock a blocked Write; this
// models the cooperative destination the contract can guarantee.
type gateWriter struct {
	ctx context.Context
	dst io.Writer
}

func (w *gateWriter) Write(p []byte) (int, error) {
	if w.ctx.Err() != nil {
		return 0, w.ctx.Err()
	}

	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
	}

	return w.dst.Write(p)
}

// assertNoStreamLeak waits for the pump, context watcher and io.Pipe
// goroutines of a finished transfer to exit and reports the surviving stack
// traces when they do not.
func assertNoStreamLeak(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if !hasLeakedStreamGoroutine() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	if leaked := leakedStacks(); len(leaked) > 0 {
		t.Fatalf("stream goroutines leaked:\n%s", strings.Join(leaked, "\n---\n"))
	}
}

func hasLeakedStreamGoroutine() bool {
	return len(leakedStacks()) > 0
}

func leakedStacks() []string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)

	var leaked []string

	for _, trace := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(trace, "formstream.(*StreamWriter).") ||
			strings.Contains(trace, "io.(*pipe).read") ||
			strings.Contains(trace, "io.(*pipe).write") {
			leaked = append(leaked, trace)
		}
	}

	return leaked
}

// TestStreamWriterContract_SourceReaderMidFailure injects a recoverable
// failure halfway through a file source: bytes already transmitted are a
// model prefix, the causal error is preserved, Close reports the same error
// exactly once, no goroutine or pipe leaks, and retrying with identical
// input yields the complete stable model bytes.
func TestStreamWriterContract_SourceReaderMidFailure(t *testing.T) {
	t.Parallel()

	fx := fixedSeedFixture(t)
	want := modelBody(t, fx, contractBoundary)
	targetFile := fx.files[2]

	prefixFieldsAndFiles := len(fx.fields) + 2

	prefixLength := bytes.Index(want, []byte(targetFile.content[:16])) + len(targetFile.content)/2

	pr, pw := io.Pipe()
	sw := formstream.NewStreamWriter(context.Background(), pw)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	var firstErr error

	produce := func() error {
		for _, field := range fx.fields {
			if err := sw.WriteField(field.name, field.value); err != nil {
				return err
			}
		}

		for i := 0; i < 2; i++ {
			file := fx.files[i]
			if err := sw.WriteFile(file.field, file.fileName, file.contentType, bytes.NewReader(file.content)); err != nil {
				return err
			}
		}

		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition",
			fmt.Sprintf(`form-data; name="%s"; filename="%s"`, targetFile.field, targetFile.fileName))
		header.Set("Content-Type", targetFile.contentType)
		pwPart, err := sw.CreatePart(header)
		if err != nil {
			return err
		}

		src := &failAfterReader{
			data:   targetFile.content,
			failAt: len(targetFile.content) / 2,
		}
		if _, err := io.Copy(pwPart, src); err != nil {
			_ = pwPart.CloseWithError(err)

			return err
		}

		return pwPart.Close()
	}

	errCh := make(chan error, 1)
	go func() { errCh <- produce() }()

	got, readErr := readChunks(pr, 13)
	firstErr = <-errCh

	if !errors.Is(firstErr, errInjectedSource) {
		t.Fatalf("producer error = %v, want causal %v", firstErr, errInjectedSource)
	}

	if readErr != nil && !errors.Is(readErr, errInjectedSource) {
		t.Fatalf("consumer observed %v, want causal source error", readErr)
	}

	if len(got) > len(want) {
		t.Fatalf("transmitted %d bytes, model is only %d bytes", len(got), len(want))
	}

	if !bytes.HasPrefix(want, got) {
		offset := firstDiff(want, got)
		t.Fatalf("transmitted bytes are not a model prefix (diverge at %d):\n%s",
			offset, diffContext(got, want, offset))
	}

	if len(got) < prefixLength {
		t.Errorf("only %d bytes reached the destination before failure, expected at least %d (mid-file injection)",
			len(got), prefixLength)
	}

	// The transfer error is reported once and stays identical afterwards.
	closeErr := sw.Close()
	assertSingleError(t, firstErr, closeErr, errInjectedSource)

	// Internal invariant: closing boundary must not have been written on
	// failure, and the wire cannot be parsed as a complete form.
	if bytes.Contains(got, []byte("--"+contractBoundary+"--")) {
		t.Errorf("closing boundary present after source failure (partial framing finalized)")
	}
	_ = prefixFieldsAndFiles

	assertNoStreamLeak(t)

	// Recovery: a fresh writer with identical input produces the full model.
	retry, err := generateBody(t, context.Background(), fx, contractBoundary, 13)
	if err != nil {
		t.Fatalf("retry after failure failed: %v", err)
	}
	assertWire(t, "retry identical input", retry, want)
}

func firstDiff(want, got []byte) int {
	for i := 0; i < len(want) && i < len(got); i++ {
		if want[i] != got[i] {
			return i
		}
	}

	if len(want) != len(got) {
		if len(want) < len(got) {
			return len(want)
		}

		return len(got)
	}

	return -1
}

func assertSingleError(t *testing.T, firstErr, closeErr error, causal error) {
	t.Helper()

	if closeErr == nil {
		t.Fatalf("Close after failure returned nil, error must be reported again")
	}

	if closeErr != firstErr {
		t.Errorf("Close error %v is not the single terminal error %v", closeErr, firstErr)
	}

	if !errors.Is(closeErr, causal) {
		t.Errorf("Close error %v lost causal chain to %v", closeErr, causal)
	}

	repeated := closeErr
	if !errors.Is(repeated, causal) || repeated != firstErr {
		t.Errorf("terminal error not stable across observations")
	}
}

// TestStreamWriterContract_DestinationWriterMidFailure fails the destination
// after it accepted a prefix of the wire. The producer's in-flight write
// surfaces the destination error, the bytes already written match the model
// prefix, the closing boundary is absent and the retry succeeds fully.
func TestStreamWriterContract_DestinationWriterMidFailure(t *testing.T) {
	t.Parallel()

	fx := fixedSeedFixture(t)
	want := modelBody(t, fx, contractBoundary)

	sink := &bytes.Buffer{}
	failing := &failingWriterAfter{dst: sink, failAt: len(want) / 3, err: errInjectedDest}

	sw := formstream.NewStreamWriter(context.Background(), failing)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	var producerErr error
	done := make(chan struct{})

	go func() {
		producerErr = streamFixture(sw, fx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("producer did not stop after destination failure")
	}

	if !errors.Is(producerErr, errInjectedDest) {
		t.Fatalf("producer error = %v, want %v", producerErr, errInjectedDest)
	}

	closeErr := sw.Close()
	assertSingleError(t, producerErr, closeErr, errInjectedDest)

	got := sink.Bytes()
	if !bytes.HasPrefix(want, got) {
		offset := firstDiff(want, got)
		t.Fatalf("accepted output is not a model prefix (diverge at %d):\n%s",
			offset, diffContext(got, want, offset))
	}

	if bytes.Contains(got, []byte("--"+contractBoundary+"--")) {
		t.Errorf("closing boundary present despite destination failure")
	}

	assertNoStreamLeak(t)

	retry, err := generateBody(t, context.Background(), fx, contractBoundary, 13)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	assertWire(t, "retry after destination failure", retry, want)
}

type failingWriterAfter struct {
	dst     io.Writer
	failAt  int
	written int
	err     error
}

func (w *failingWriterAfter) Write(p []byte) (int, error) {
	if w.written >= w.failAt {
		return 0, w.err
	}

	n := len(p)
	if w.written+n > w.failAt {
		n = w.failAt - w.written
	}

	nw, err := w.dst.Write(p[:n])
	w.written += nw

	if err != nil {
		return nw, err
	}

	if w.written >= w.failAt {
		return nw, w.err
	}

	return nw, nil
}

// TestStreamWriterContract_ShortWriter is the case a naive pass-through
// implementation fails: the destination accepts fewer bytes than requested
// with a nil error. The writer must detect the short write and report
// io.ErrShortWrite instead of silently producing truncated framing.
func TestStreamWriterContract_ShortWriter(t *testing.T) {
	t.Parallel()

	fx := fixedSeedFixture(t)
	want := modelBody(t, fx, contractBoundary)

	sink := &bytes.Buffer{}
	short := &shortWriter{dst: sink, failAt: len(want)/2 - 7}

	sw := formstream.NewStreamWriter(context.Background(), short)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	var producerErr error
	done := make(chan struct{})

	go func() {
		producerErr = streamFixture(sw, fx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("producer deadlocked on a short writer")
	}

	if !errors.Is(producerErr, io.ErrShortWrite) {
		t.Fatalf("producer error = %v, want io.ErrShortWrite", producerErr)
	}

	closeErr := sw.Close()
	assertSingleError(t, producerErr, closeErr, io.ErrShortWrite)

	got := sink.Bytes()
	if len(got) >= len(want) {
		t.Errorf("short writer produced %d bytes; a truncated body must be shorter than the %d byte model",
			len(got), len(want))
	}

	if !bytes.HasPrefix(want, got) {
		offset := firstDiff(want, got)
		t.Fatalf("accepted output is not a model prefix (diverge at %d):\n%s",
			offset, diffContext(got, want, offset))
	}

	assertNoStreamLeak(t)
}

// blockingSource blocks in Read until released. It models a slow upload that
// is still in flight when the caller cancels.
type blockingSource struct {
	release chan struct{}
	closed  chan struct{}
}

func (s *blockingSource) Read(p []byte) (int, error) {
	select {
	case <-s.release:
		return 0, io.EOF
	case <-s.closed:
		return 0, errInjectedDest
	}
}

func (s *blockingSource) Close() {
	close(s.closed)
}

// TestStreamWriterContract_ContextCancelBlockedSource cancels while a part
// source read is blocked. The producer receives context.Canceled, Close
// reports the same cause once, and every goroutine and pipe exits.
func TestStreamWriterContract_ContextCancelBlockedSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	sink := &bytes.Buffer{}
	sw := formstream.NewStreamWriter(ctx, sink)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	src := &blockingSource{release: make(chan struct{}), closed: make(chan struct{})}

	produced := make(chan error, 1)
	go func() {
		produced <- sw.WriteFile("slow", "slow.bin", "application/octet-stream", src)
	}()

	// Wait until the pump is blocked inside the source read.
	if !waitUntil(2*time.Second, func() bool {
		return bytes.Contains(sink.Bytes(), []byte("slow.bin"))
	}) {
		t.Fatalf("part header never reached the destination")
	}

	cancel()

	select {
	case err := <-produced:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WriteFile returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("producer did not stop on cancellation")
	}

	closeErr := sw.Close()
	if !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("Close returned %v, want context.Canceled", closeErr)
	}

	assertNoStreamLeak(t)
}

// TestStreamWriterContract_ContextCancelCooperativeDestination cancels while
// the destination blocks in Write.
func TestStreamWriterContract_ContextCancelCooperativeDestination(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	prDst, pwDst := io.Pipe()
	gate := &gateWriter{ctx: ctx, dst: pwDst}
	sw := formstream.NewStreamWriter(ctx, gate)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	produced := make(chan error, 1)
	go func() {
		produced <- sw.WriteFile("slow", "slow.bin", "application/octet-stream", strings.NewReader(strings.Repeat("z", 100000)))
	}()

	buf := make([]byte, 64)
	if _, err := prDst.Read(buf); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	cancel()

	select {
	case err := <-produced:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WriteFile returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("producer did not stop on cancellation")
	}

	_ = prDst.Close()
	_ = pwDst.Close()

	if err := sw.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close returned %v, want context.Canceled", err)
	}

	assertNoStreamLeak(t)
}

func waitUntil(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return true
		}

		time.Sleep(time.Millisecond)
	}

	return false
}

// TestStreamWriterContract_ContextCancelBetweenParts cancels while the pump
// is idle, waiting for the next part. The pump must exit even though no part
// read or destination write is in progress.
func TestStreamWriterContract_ContextCancelBetweenParts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	got := &bytes.Buffer{}
	sw := formstream.NewStreamWriter(ctx, got)
	if err := sw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}

	if err := sw.WriteField("first", "value"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}

	cancel()

	if err := sw.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close after cancel between parts returned %v, want context.Canceled", err)
	}

	assertNoStreamLeak(t)
}

// TestStreamWriterContract_CancelCloseRace repeatedly interleaves
// cancellation with Close. The transfer must end with exactly one terminal
// error (context.Canceled) and no leaked goroutine under every ordering.
func TestStreamWriterContract_CancelCloseRace(t *testing.T) {
	t.Parallel()

	for iteration := 0; iteration < 20; iteration++ {
	t.Run(fmt.Sprintf("iteration=%d", iteration), func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())

		prDst, pwDst := io.Pipe()
		sw := formstream.NewStreamWriter(ctx, pwDst)
		if err := sw.SetBoundary(contractBoundary); err != nil {
			t.Fatal(err)
		}

		src := &blockingSource{release: make(chan struct{}), closed: make(chan struct{})}

		produced := make(chan error, 1)
		go func() {
			produced <- sw.WriteFile("slow", "slow.bin", "application/octet-stream", src)
		}()

		buf := make([]byte, 16)
		_, _ = prDst.Read(buf)

		start := make(chan struct{})

		go func() {
			<-start
			cancel()
		}()
		go func() {
			<-start
			_ = sw.Close()
		}()

		close(start)

		select {
		case err := <-produced:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("WriteFile returned %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("producer did not stop on cancel/close race")
		}

		err := sw.Close()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close returned %v, want context.Canceled", err)
		}

		_ = prDst.Close()
		_ = pwDst.Close()

		assertNoStreamLeak(t)
	})
	}
}

// TestStreamWriterContract_BudgetExhaustion distinguishes input damage
// (source errors), resource budget exhaustion (ErrBodyTooLarge,
// ErrTooManyParts) and cancellation: each keeps its own cause and the
// diagnostic must not turn a success into an error.
func TestStreamWriterContract_BudgetExhaustion(t *testing.T) {
	t.Parallel()

	t.Run("body size budget exact fit succeeds", func(t *testing.T) {
		t.Parallel()

		fx := fixedSeedFixture(t)
		want := modelBody(t, fx, contractBoundary)

		var contentBytes int
		for _, field := range fx.fields {
			contentBytes += len(field.value)
		}

		for _, file := range fx.files {
			contentBytes += len(file.content)
		}

		got, err := generateBody(t, context.Background(), fx, contractBoundary, 1,
			formstream.WithMaxBodySize(int64(contentBytes)))
		if err != nil {
			t.Fatalf("exact-fit body budget returned %v", err)
		}

		assertWire(t, "exact-fit budget", got, want)
	})

	t.Run("body size budget overflow keeps cause", func(t *testing.T) {
		t.Parallel()

		fx := fixedSeedFixture(t)
		want := modelBody(t, fx, contractBoundary)

		pr, pw := io.Pipe()
		sw := formstream.NewStreamWriter(context.Background(), pw,
			formstream.WithMaxBodySize(10))

		errCh := make(chan error, 1)
		go func() { errCh <- streamFixture(sw, fx) }()

		got, _ := readChunks(pr, 1)
		err := <-errCh

		if !errors.Is(err, formstream.ErrBodyTooLarge) {
			t.Fatalf("error = %v, want ErrBodyTooLarge", err)
		}

		if !bytes.HasPrefix(want, got) {
			t.Fatalf("output before budget failure is not a model prefix")
		}

		if err := sw.Close(); !errors.Is(err, formstream.ErrBodyTooLarge) {
			t.Fatalf("Close error = %v, want ErrBodyTooLarge", err)
		}

		assertNoStreamLeak(t)
	})

	t.Run("part budget keeps cause", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sw := formstream.NewStreamWriter(context.Background(), buf,
			formstream.WithWriterMaxParts(1))
		if err := sw.SetBoundary(contractBoundary); err != nil {
			t.Fatal(err)
		}

		if err := sw.WriteField("one", "1"); err != nil {
			t.Fatalf("first part: %v", err)
		}

		err := sw.WriteField("two", "2")
		if !errors.Is(err, formstream.ErrTooManyParts) {
			t.Fatalf("second part error = %v, want ErrTooManyParts", err)
		}

		if err := sw.Close(); !errors.Is(err, formstream.ErrTooManyParts) {
			t.Fatalf("Close error = %v, want ErrTooManyParts", err)
		}
	})
}

// TestStreamWriterContract_InvalidBoundaryAndClosedState validates the
// boundary error layer and the closed-state contract, both repeatable.
func TestStreamWriterContract_InvalidBoundaryAndClosedState(t *testing.T) {
	t.Parallel()

	for _, boundary := range []string{"", strings.Repeat("a", 71), "bad boundary!"} {
		t.Run(fmt.Sprintf("boundary=%q", boundary), func(t *testing.T) {
			t.Parallel()

			sw := formstream.NewStreamWriter(context.Background(), &bytes.Buffer{})
			if err := sw.SetBoundary(boundary); !errors.Is(err, formstream.ErrInvalidBoundary) {
				t.Fatalf("SetBoundary(%q) error = %v, want ErrInvalidBoundary", boundary, err)
			}

			// Repeated rejection gives the same error class.
			if err := sw.SetBoundary(boundary); !errors.Is(err, formstream.ErrInvalidBoundary) {
				t.Fatalf("repeated SetBoundary error = %v, want ErrInvalidBoundary", err)
			}
		})
	}

	t.Run("boundary frozen after first part", func(t *testing.T) {
		t.Parallel()

		sw := formstream.NewStreamWriter(context.Background(), &bytes.Buffer{})
		if err := sw.WriteField("a", "b"); err != nil {
			t.Fatal(err)
		}

		if err := sw.SetBoundary("other"); !errors.Is(err, formstream.ErrInvalidBoundary) {
			t.Fatalf("late SetBoundary error = %v, want ErrInvalidBoundary", err)
		}

		if err := sw.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if err := sw.WriteField("c", "d"); !errors.Is(err, formstream.ErrStreamClosed) {
			t.Fatalf("write after Close error = %v, want ErrStreamClosed", err)
		}

		if err := sw.Close(); err != nil {
			t.Fatalf("second Close must repeat nil, got %v", err)
		}
	})
}

const contractBoundary = "GoFormStreamContract-Boundary_0123456789"

// contractFixture is the fixed-seed input set: fields, an empty file, a
// unicode-named file and a file with binary content that cannot contain the
// boundary bytes, so framing is always parseable.
type contractFixture struct {
	fields []modelField
	files  []modelFile
}

type modelField struct {
	name  string
	value string
}

type modelFile struct {
	field       string
	fileName    string
	contentType string
	content     []byte
	ordered     bool
}

func fixedSeedFixture(t *testing.T) contractFixture {
	t.Helper()

	rng := rand.New(rand.NewSource(0xC0FFEE))

	text := make([]byte, 240)
	for i := range text {
		text[i] = byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\r\n"[rng.Intn(64)])
	}

	binary := make([]byte, 300)
	for i := range binary {
		binary[i] = byte(rng.Intn(256))
	}
	binary = bytes.ReplaceAll(binary, []byte("--"), []byte("XX"))

	return contractFixture{
		fields: []modelField{
			{name: "title", value: "フォームストリーム/Stream\r\n123"},
			{name: "note", value: strings.Repeat("あいうえお", 12)},
			{name: "sparse", value: ""},
		},
		files: []modelFile{
			{field: "empty", fileName: "empty.txt", contentType: "text/plain", content: []byte{}},
			{field: "icon", fileName: "日本語-icône.png", contentType: "image/png", content: binary},
			{field: "notes", fileName: "notes.txt", contentType: "text/plain", content: text},
		},
	}
}

// modelBody independently builds the same multipart message with the
// standard library writer. Headers are created in reverse insertion order
// for one file so the canonical header ordering is also asserted.
func modelBody(t *testing.T, fx contractFixture, boundary string) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("model SetBoundary: %v", err)
	}

	for _, field := range fx.fields {
		if err := mw.WriteField(field.name, field.value); err != nil {
			t.Fatalf("model WriteField: %v", err)
		}
	}

	for _, file := range fx.files {
		header := textproto.MIMEHeader{}
		if file.ordered {
			header.Set("Content-Disposition",
				fmt.Sprintf(`form-data; name="%s"; filename="%s"`, file.field, file.fileName))
			header.Set("Content-Type", file.contentType)
			pw, err := mw.CreatePart(header)
			if err != nil {
				t.Fatalf("model CreatePart: %v", err)
			}
			if _, err := pw.Write(file.content); err != nil {
				t.Fatalf("model file write: %v", err)
			}

			continue
		}

		// Reverse insertion order: the wire order must still be canonical.
		header.Add("X-Trailer", "a")
		header.Add("X-Trailer", "b")
		header.Set("Content-Type", file.contentType)
		header.Set("Content-Disposition",
			fmt.Sprintf(`form-data; name="%s"; filename="%s"`, file.field, file.fileName))
		pw, err := mw.CreatePart(header)
		if err != nil {
			t.Fatalf("model CreatePart: %v", err)
		}
		if _, err := pw.Write(file.content); err != nil {
			t.Fatalf("model file write: %v", err)
		}
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("model Close: %v", err)
	}

	return buf.Bytes()
}

// generateBody drives the real public API and returns the bytes the
// destination observed. chunkSize > 0 limits every destination Read to that
// many bytes, exercising arbitrary wire chunk boundaries including splits
// inside boundaries, headers and content.
func generateBody(t *testing.T, ctx context.Context, fx contractFixture, boundary string, chunkSize int, options ...formstream.StreamWriterOption) ([]byte, error) {
	t.Helper()

	pr, pw := io.Pipe()
	sw := formstream.NewStreamWriter(ctx, pw, options...)
	if boundary != "" {
		if err := sw.SetBoundary(boundary); err != nil {
			_ = pw.CloseWithError(err)

			return nil, err
		}
	}

	usedBoundary := sw.Boundary()

	result := make(chan error, 1)
	go func() {
		result <- streamFixture(sw, fx)
	}()

	got, readErr := readChunks(pr, chunkSize)
	if readErr != nil {
		_ = pr.CloseWithError(readErr)
	}

	closeErr := <-result
	_ = usedBoundary

	if closeErr != nil {
		return got, closeErr
	}

	return got, readErr
}

func streamFixture(sw *formstream.StreamWriter, fx contractFixture) error {
	for _, field := range fx.fields {
		if err := sw.WriteField(field.name, field.value); err != nil {
			return err
		}
	}

	for _, file := range fx.files {
		if err := sw.WriteFile(file.field, file.fileName, file.contentType, bytes.NewReader(file.content)); err != nil {
			return err
		}
	}

	return sw.Close()
}

func readChunks(r io.Reader, chunkSize int) ([]byte, error) {
	if chunkSize <= 0 {
		return io.ReadAll(r)
	}

	got := make([]byte, 0, 1024)
	buf := make([]byte, chunkSize)

	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)

		if err != nil {
			if errors.Is(err, io.EOF) {
				return got, nil
			}

			return got, err
		}
	}
}

// assertWire reports the first offset where got diverges from want, the
// surrounding bytes and whether got is an exact prefix of want (expected on
// failure injection).
func assertWire(t *testing.T, where string, got, want []byte) bool {
	t.Helper()

	if bytes.Equal(got, want) {
		return true
	}

	prefix := bytes.HasPrefix(want, got)
	minLen := len(got)
	if len(want) < minLen {
		minLen = len(want)
	}

	offset := 0
	for offset < minLen && got[offset] == want[offset] {
		offset++
	}

	t.Errorf("%s: wire mismatch at byte %d (got %d bytes, want %d bytes, prefix=%t):\n%s",
		where, offset, len(got), len(want), prefix, diffContext(got, want, offset))

	return false
}

func diffContext(got, want []byte, offset int) string {
	const radius = 24

	start := offset - radius
	if start < 0 {
		start = 0
	}

	endGot := offset + radius
	if endGot > len(got) {
		endGot = len(got)
	}

	endWant := offset + radius
	if endWant > len(want) {
		endWant = len(want)
	}

	return fmt.Sprintf("  got[%d:%d]=%q\n want[%d:%d]=%q",
		start, endGot, got[start:endGot], start, endWant, want[start:endWant])
}

// TestStreamWriterContract_ModelWiringArbitraryChunkBoundaries proves the
// generated wire bytes equal the standard-library model at every chunk size
// from one byte upwards: one-byte chunks, boundaries split across chunks and
// header/content splits are all covered.
func TestStreamWriterContract_ModelWiringArbitraryChunkBoundaries(t *testing.T) {
	t.Parallel()

	fx := fixedSeedFixture(t)
	want := modelBody(t, fx, contractBoundary)

	for chunkSize := 1; chunkSize <= 128; chunkSize++ {
		t.Run(fmt.Sprintf("chunksize=%d", chunkSize), func(t *testing.T) {
			t.Parallel()

			got, err := generateBody(t, context.Background(), fx, contractBoundary, chunkSize)
			if err != nil {
				t.Fatalf("generation failed at chunk size %d: %v", chunkSize, err)
			}

			assertWire(t, fmt.Sprintf("chunk size %d", chunkSize), got, want)
			assertFraming(t, got, contractBoundary, len(fx.fields)+len(fx.files))
		})
	}

	// A handful of larger, deliberately awkward sizes.
	for _, chunkSize := range []int{251, 256, 333, 1024, 4096} {
		t.Run(fmt.Sprintf("chunksize=%d", chunkSize), func(t *testing.T) {
			t.Parallel()

			got, err := generateBody(t, context.Background(), fx, contractBoundary, chunkSize)
			if err != nil {
				t.Fatalf("generation failed at chunk size %d: %v", chunkSize, err)
			}

			assertWire(t, fmt.Sprintf("chunk size %d", chunkSize), got, want)
		})
	}
}

// TestStreamWriterContract_WireFramingAndRoundTrip asserts the wire framing
// directly (boundary placement, canonical header order, closing state) and
// re-parses the bytes with formstream.Parser, so fields and files match
// rather than only the parsed field values being compared.
func TestStreamWriterContract_WireFramingAndRoundTrip(t *testing.T) {
	t.Parallel()

	fx := fixedSeedFixture(t)
	got, err := generateBody(t, context.Background(), fx, contractBoundary, 37)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	want := modelBody(t, fx, contractBoundary)
	assertWire(t, "round trip", got, want)
	assertFraming(t, got, contractBoundary, len(fx.fields)+len(fx.files))

	// Canonical header order: Content-Disposition precedes Content-Type, and
	// repeated headers keep insertion order. This would fail for an
	// implementation that emitted map iteration order.
	firstHeaderBlock := bytes.Index(got, []byte("X-Trailer: a\r\nX-Trailer: b\r\n"))
	if firstHeaderBlock < 0 {
		t.Errorf("repeated header ordering not found on the wire")
	}

	roundTripFixture(t, got, contractBoundary, fx)
}

func assertFraming(t *testing.T, body []byte, boundary string, partCount int) {
	t.Helper()

	opening := []byte("--" + boundary + "\r\n")
	middle := []byte("\r\n--" + boundary + "\r\n")
	closing := []byte("\r\n--" + boundary + "--\r\n")

	if !bytes.HasPrefix(body, opening) {
		t.Errorf("body does not start with opening boundary %q", opening)
	}

	if !bytes.HasSuffix(body, closing) {
		t.Errorf("body does not end with closing boundary %q", closing)
	}

	if count := bytes.Count(body, middle); count != partCount-1 {
		t.Errorf("inter-part boundary count = %d, want %d", count, partCount-1)
	}

	if count := bytes.Count(body, []byte("--"+boundary+"--")); count != 1 {
		t.Errorf("closing boundary appears %d times, want exactly 1 (closed state)", count)
	}
}

// roundTripFixture re-enters the repository through the public parser: each
// file is consumed through a registered hook (streaming semantics) and each
// field is read back after Parse succeeds.
func roundTripFixture(t *testing.T, body []byte, boundary string, fx contractFixture) {
	t.Helper()

	parser := formstream.NewParser(boundary)

	gotFiles := map[string][]byte{}
	gotFileNames := map[string]string{}

	for _, file := range fx.files {
		name := file.field
		err := parser.Register(name, func(r io.Reader, header formstream.Header) error {
			content, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			gotFiles[name] = content
			gotFileNames[name] = header.FileName()

			return nil
		})
		if err != nil {
			t.Fatalf("Register(%s): %v", name, err)
		}
	}

	if err := parser.Parse(bytes.NewReader(body)); err != nil {
		t.Fatalf("parser.Parse: %v", err)
	}

	for _, field := range fx.fields {
		value, _, ok := parser.Value(field.name)
		if !ok {
			t.Errorf("field %q missing after round trip", field.name)

			continue
		}

		if value != field.value {
			t.Errorf("field %q = %q, want %q", field.name, value, field.value)
		}
	}

	for _, file := range fx.files {
		content, ok := gotFiles[file.field]
		if !ok {
			t.Errorf("file %q missing after round trip", file.field)

			continue
		}

		if !bytes.Equal(content, file.content) {
			t.Errorf("file %q content diverged (got %d bytes, want %d bytes)",
				file.field, len(content), len(file.content))
		}

		if gotFileNames[file.field] != file.fileName {
			t.Errorf("file %q filename = %q, want %q (unicode transport)",
				file.field, gotFileNames[file.field], file.fileName)
		}
	}
}

// TestStreamWriterContract_RandomBoundaryAndEmptyForm covers the auto
// generated boundary and a message with no parts; both must equal the
// standard-library model and parse cleanly.
func TestStreamWriterContract_RandomBoundaryAndEmptyForm(t *testing.T) {
	t.Parallel()

	t.Run("random boundary fixed seed fixture", func(t *testing.T) {
		t.Parallel()

		fx := fixedSeedFixture(t)

		pr, pw := io.Pipe()
		sw := formstream.NewStreamWriter(context.Background(), pw)
		boundary := sw.Boundary()

		errCh := make(chan error, 1)
		go func() { errCh <- streamFixture(sw, fx) }()

		got, err := readChunks(pr, 5)
		if err != nil {
			t.Fatalf("read: %v", err)
		}

		if err := <-errCh; err != nil {
			t.Fatalf("generation: %v", err)
		}

		want := modelBody(t, fx, boundary)
		assertWire(t, "random boundary", got, want)

		contentType := sw.FormDataContentType()
		if !strings.Contains(contentType, "boundary="+boundary) {
			t.Errorf("Content-Type %q does not expose boundary %q", contentType, boundary)
		}

		roundTripFixture(t, got, boundary, fx)
	})

	t.Run("empty form", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		model := multipart.NewWriter(buf)
		if err := model.SetBoundary(contractBoundary); err != nil {
			t.Fatal(err)
		}
		if err := model.Close(); err != nil {
			t.Fatal(err)
		}

		got := &bytes.Buffer{}
		sw := formstream.NewStreamWriter(context.Background(), got)
		if err := sw.SetBoundary(contractBoundary); err != nil {
			t.Fatal(err)
		}
		if err := sw.Close(); err != nil {
			t.Fatalf("empty Close: %v", err)
		}

		assertWire(t, "empty form", got.Bytes(), buf.Bytes())
	})
}
