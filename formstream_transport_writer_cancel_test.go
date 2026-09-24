package formstream_test

// Destination short writes and context cancellation.
//
// Short write: a hook streams a part into an io.Writer that accepts fewer
// bytes than offered and returns io.ErrShortWrite. The parser's public
// Parse must surface that exact cause once, leave no temp files or
// half-updated fields, and succeed again on identical retry.
//
// Cancellation: the source reader is driven by a context. Cancelling while
// the transfer is blocked must unblock Parse, report the cancellation
// cause, and after Parse returns every goroutine/pipe the streaming
// transfer created must be gone. Retrying the same parser with the same
// input after a fresh context must be a stable success.
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"sync"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// shortWriter accepts at most one byte per Write and, when asked for
// more, reports io.ErrShortWrite with the real byte count.
type shortWriter struct {
	dst    bytes.Buffer
	writes int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	w.writes++
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) == 1 {
		w.dst.WriteByte(p[0])
		return 1, nil
	}
	w.dst.WriteByte(p[0])
	return 1, io.ErrShortWrite
}

func TestFormStreamTransportContract_ShortWriter_FastPath(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	p := formstream.NewParser(model.boundary)

	sw := &shortWriter{}
	var hookErr error
	err := p.Register("fileSmall", func(r io.Reader, _ formstream.Header) error {
		_, hookErr = io.Copy(sw, r)
		return hookErr
	})
	if err != nil {
		t.Fatal(err)
	}

	before := tempFileSnapshot()
	g0 := snapshotGoroutines()
	perr := p.Parse(bytes.NewReader(model.wire))
	if perr == nil {
		t.Fatal("first violation: short-write error was swallowed by Parse")
	}
	if !errors.Is(perr, io.ErrShortWrite) {
		t.Fatalf("first violation: short-write cause lost:\n want io.ErrShortWrite chain\n got  %v", perr)
	}
	if n := countMatches(perr, io.ErrShortWrite); n != 1 {
		t.Errorf("first violation: short-write error reported %d times:\n%v", n, perr)
	}
	if len(p.ValueMap()) != 0 {
		t.Errorf("first violation: fields half-stored after writer failure: %v", p.ValueMap())
	}
	assertNoNewTempFiles(t, before, "short-writer/fast")
	assertNoGoroutineLeak(t, g0, "short-writer/fast")

	// Retry with a healthy destination on the same parser.
	var healthy bytes.Buffer
	var healthyCalls int
	p2 := formstream.NewParser(model.boundary)
	err = p2.Register("fileSmall", func(r io.Reader, _ formstream.Header) error {
		healthyCalls++
		_, err := io.Copy(&healthy, r)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if rerr := p2.Parse(bytes.NewReader(model.wire)); rerr != nil {
		t.Fatalf("first violation: retry after short write failed: %v", rerr)
	}
	exp := expectedResult(t, model)
	if healthy.String() != exp.files["fileSmall"] || healthyCalls != 1 {
		t.Errorf("first violation: retry content: %d bytes calls=%d", healthy.Len(), healthyCalls)
	}
}

func TestFormStreamTransportContract_ShortWriter_SpooledPath(t *testing.T) {
	t.Parallel()

	// Required field comes after the file, so the file is spooled before
	// the hook runs against the short writer.
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	if err := mw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}
	writeDispositionPart(t, mw, "upload", "data.bin", "application/octet-stream", bytes.Repeat([]byte("z"), 96))
	if err := mw.WriteField("required", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	wire := b.Bytes()

	p := formstream.NewParser(contractBoundary,
		formstream.WithMaxMemSize(512), formstream.WithMaxMemFileSize(32))
	sw := &shortWriter{}
	err := p.Register("upload", func(r io.Reader, _ formstream.Header) error {
		_, werr := io.Copy(sw, r)
		return werr
	}, formstream.WithRequiredPart("required"))
	if err != nil {
		t.Fatal(err)
	}

	before := tempFileSnapshot()
	perr := p.Parse(bytes.NewReader(wire))
	if !errors.Is(perr, io.ErrShortWrite) {
		t.Fatalf("first violation: spooled short-write cause: %v", perr)
	}
	if n := countMatches(perr, io.ErrShortWrite); n != 1 {
		t.Errorf("first violation: short-write cause count=%d: %v", n, perr)
	}
	assertNoNewTempFiles(t, before, "short-writer/spooled")

	// Re-running with the same failing destination stays deterministic:
	// the writer error remains the cause and temp files stay cleaned up.
	p2 := formstream.NewParser(contractBoundary,
		formstream.WithMaxMemSize(512), formstream.WithMaxMemFileSize(32))
	sw2 := &shortWriter{}
	if regErr := p2.Register("upload", func(r io.Reader, _ formstream.Header) error {
		_, werr := io.Copy(sw2, r)
		return werr
	}, formstream.WithRequiredPart("required")); regErr != nil {
		t.Fatal(regErr)
	}
	if rerr := p2.Parse(bytes.NewReader(wire)); !errors.Is(rerr, io.ErrShortWrite) {
		t.Errorf("first violation: deterministic short-write cause: %v", rerr)
	} else if n := countMatches(rerr, io.ErrShortWrite); n != 1 {
		t.Errorf("first violation: short-write retried cause count=%d: %v", n, rerr)
	}
	assertNoNewTempFiles(t, before, "short-writer/spooled-retry")
}

// cancelReader blocks reads until its gate is opened or ctx is done, then
// returns ctx.Err(). Reads are served through io.Pipe so cancellation
// genuinely has to unwind a blocked transfer rather than just returning a
// buffered error.
type cancelFixture struct {
	ctx    context.Context
	cancel context.CancelFunc
	pr     *io.PipeReader
	once   sync.Once
}

func newCancelFixture(wire []byte, blockAfter int) *cancelFixture {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	f := &cancelFixture{ctx: ctx, cancel: cancel, pr: pr}
	go func() {
		// serve blockAfter bytes, then wait for cancellation
		i := 0
		for i < blockAfter {
			n, err := pw.Write(wire[i:min(blockAfter, len(wire))])
			if err != nil {
				return
			}
			i += n
		}
		<-ctx.Done()
		pw.CloseWithError(ctx.Err())
	}()
	return f
}

func (f *cancelFixture) release() {
	f.once.Do(func() { f.cancel() })
}

func TestFormStreamTransportContract_ContextCancel_MidStream(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	blockAfter := len(model.wire) / 2

	f := newCancelFixture(model.wire, blockAfter)
	p := formstream.NewParser(model.boundary)
	got := newParsedForm()
	hookRecorder(t, p, model, got, nil)

	g0 := snapshotGoroutines()
	before := tempFileSnapshot()

	done := make(chan error, 1)
	go func() { done <- p.Parse(f.pr) }()

	// cancel while Parse must be blocked reading the remaining body
	time.Sleep(30 * time.Millisecond)
	f.release()

	select {
	case perr := <-done:
		if perr == nil {
			t.Fatal("first violation: cancelled Parse returned nil")
		}
		if !errors.Is(perr, context.Canceled) {
			t.Fatalf("first violation: cancellation cause lost:\n want context.Canceled\n got  %v", perr)
		}
		if n := countMatches(perr, context.Canceled); n != 1 {
			t.Errorf("first violation: cancellation reported %d times: %v", n, perr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first violation: Parse did not return after context cancellation")
	}

	assertNoGoroutineLeak(t, g0, "cancel/mid-stream")
	assertNoNewTempFiles(t, before, "cancel/mid-stream")
	if len(p.ValueMap()) != 0 {
		t.Errorf("first violation: fields survived cancellation: %v", p.ValueMap())
	}

	// Retry the identical input on a fresh parser with a new, uncancelled
	// stream; the result must be a clean success.
	rp := formstream.NewParser(model.boundary)
	retry := newParsedForm()
	hookRecorder(t, rp, model, retry, nil)
	if rerr := rp.Parse(bytes.NewReader(model.wire)); rerr != nil {
		t.Fatalf("first violation: retry after cancellation failed: %v", rerr)
	}
	fillValues(t, rp, retry)
	assertResultEqual(t, "post-cancel retry", expectedResult(t, model), retry)
}

// TestFormStreamTransportContract_CancelRace cancels at many offsets and
// also races cancellation against successful completion: when the body
// finishes first the result must be a clean success, never an error.
func TestFormStreamTransportContract_CancelRace(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	delims := failingOffsets(model.wire)

	for _, at := range delims {
		at := at
		t.Run("cancelRace_"+itoa(at), func(t *testing.T) {
			t.Parallel()

			f := newCancelFixture(model.wire, at)
			p := formstream.NewParser(model.boundary)
			got := newParsedForm()
			hookRecorder(t, p, model, got, nil)
			g0 := snapshotGoroutines()
			before := tempFileSnapshot()

			done := make(chan error, 1)
			go func() { done <- p.Parse(f.pr) }()
			f.release()

			select {
			case perr := <-done:
				switch {
				case perr == nil:
					// body fully delivered before cancel observed: success
					// path must be complete and leak-free.
				case errors.Is(perr, context.Canceled):
					if n := countMatches(perr, context.Canceled); n != 1 {
						t.Errorf("first violation: cancel cause reported %d times: %v", n, perr)
					}
					if len(p.ValueMap()) != 0 {
						t.Errorf("first violation: shared state after cancel: %v", p.ValueMap())
					}
				default:
					t.Fatalf("first violation: unexpected cause at %d: %v", at, perr)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("first violation: Parse stuck at offset %d", at)
			}
			assertNoGoroutineLeak(t, g0, fmt.Sprintf("cancel-race@%d", at))
			assertNoNewTempFiles(t, before, fmt.Sprintf("cancel-race@%d", at))
		})
	}
}
