package formtest

import (
	"context"
	"io"
	"sync"
	"time"
)

// ChunkReader serves an in-memory body in exactly the given chunk sizes.
// Every Read returns at most schedule[i] bytes; when pace > 0 the reads are
// paced to widen scheduling windows in race runs.
type ChunkReader struct {
	data     []byte
	schedule Schedule
	pos      int
	step     int
	pace     time.Duration
}

// NewChunkReader wraps data with the given chunk schedule.
func NewChunkReader(data []byte, schedule Schedule, pace time.Duration) *ChunkReader {
	return &ChunkReader{data: data, schedule: schedule, pace: pace}
}

// Read implements io.Reader.
func (r *ChunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if r.pace > 0 {
		time.Sleep(r.pace)
	}
	n := len(r.data) - r.pos
	if r.step < len(r.schedule) && r.schedule[r.step] < n {
		n = r.schedule[r.step]
	}
	r.step++
	if n > len(p) {
		n = len(p)
	}
	n = copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// FailingReader returns the prefix of a body verbatim, then keeps failing
// with the given permanent error (a single concrete error value, so callers
// can verify the cause is reported exactly once). The failure is recoverable
// at the transport level in the sense that retrying with a fresh reader over
// a fresh parser must succeed: the parser never owns the source.
type FailingReader struct {
	data     []byte
	failAt   int64
	pos      int64
	err      error
	attempts int
}

// NewFailingReader fails permanently after failAt bytes.
func NewFailingReader(data []byte, failAt int64, err error) *FailingReader {
	if err == nil {
		err = ErrSourceFailed
	}
	return &FailingReader{data: data, failAt: failAt, err: err}
}

// Read implements io.Reader.
func (r *FailingReader) Read(p []byte) (int, error) {
	r.attempts++
	if r.pos >= r.failAt {
		return 0, r.err
	}
	n := len(p)
	remaining := int(r.failAt - r.pos)
	if n > remaining {
		n = remaining
	}
	if n > len(r.data)-int(r.pos) {
		n = len(r.data) - int(r.pos)
	}
	n = copy(p, r.data[r.pos:int(r.pos)+n])
	r.pos += int64(n)
	if r.pos >= r.failAt {
		return n, r.err
	}
	return n, nil
}

// Attempts reports how many Read calls the parser made after the failure
// point. A well-behaved parser stops draining the source after the first
// permanent error.
func (r *FailingReader) Attempts() int {
	return r.attempts
}

// RecorderWriter is a target writer that captures everything and can fail
// after accepting failAfter bytes. When shortWrite is true it advertises a
// short count (n < len(p), nil), the classic forbidden io.Writer contract
// violation; hooks must surface a short-write error instead of silently
// truncating output.
type RecorderWriter struct {
	mu         sync.Mutex
	buf        []byte
	failAfter  int
	shortWrite bool
	err        error
	pace       time.Duration
	writes     int
	accepted   int
}

// NewRecorderWriter creates a writer that fails after failAfter accepted
// bytes (-1 = never fails).
func NewRecorderWriter(failAfter int, shortWrite bool, pace time.Duration) *RecorderWriter {
	return &RecorderWriter{failAfter: failAfter, shortWrite: shortWrite, err: ErrWriterFailed, pace: pace}
}

// Write implements io.Writer.
func (w *RecorderWriter) Write(p []byte) (int, error) {
	if w.pace > 0 {
		time.Sleep(w.pace)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	if w.failAfter >= 0 && w.accepted >= w.failAfter {
		return 0, w.err
	}
	if w.shortWrite && len(p) > 1 {
		n := len(p) - 1
		w.buf = append(w.buf, p[:n]...)
		w.accepted += n
		return n, nil
	}
	w.buf = append(w.buf, p...)
	w.accepted += len(p)
	return len(p), nil
}

// Bytes returns a copy of everything the writer accepted.
func (w *RecorderWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]byte, len(w.buf))
	copy(out, w.buf)
	return out
}

// Accepted reports how many payload bytes the writer accepted in total.
func (w *RecorderWriter) Accepted() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accepted
}

// Writes reports the number of Write calls observed.
func (w *RecorderWriter) Writes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

// PipeCancel is the result side of a transport pipe. Parse reads from Reader;
// the producer goroutine pushes body bytes through Feed and always closes the
// write side exactly once.
type PipeCancel struct {
	pr *io.PipeReader
	pw *io.PipeWriter

	mu       sync.Mutex
	closed   bool
	closeErr error

	doneOnce sync.Once
	done     chan struct{}
}

// NewPipeCancel wires a pipe and starts the producer goroutine.
//
//   - ctx is observed between every byte chunk; when it is canceled the
//     producer closes the write side with the context cause,
//   - when cancelAfter >= 0 the producer cancels ctx itself after pushing
//     that many bytes (deterministic mid-stream cancellation); cancelAfter
//     of -1 leaves cancellation entirely to the caller.
//
// The returned cancel function is idempotent.
func NewPipeCancel(parent context.Context, data []byte, pace time.Duration, cancelAfter int) (*PipeCancel, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	pr, pw := io.Pipe()
	pc := &PipeCancel{pr: pr, pw: pw, done: make(chan struct{})}

	go pc.produce(ctx, data, pace, cancelAfter, cancel)

	return pc, func() {
		cancel()
	}
}

func (pc *PipeCancel) produce(ctx context.Context, data []byte, pace time.Duration, cancelAfter int, cancel context.CancelFunc) {
	defer pc.finish(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
	defer cancel()

	pos := 0
	const step = 1
	for pos < len(data) {
		if err := ctx.Err(); err != nil {
			pc.finish(func() error { return err })
			return
		}
		end := pos + step
		if end > len(data) {
			end = len(data)
		}
		n, err := pc.pw.Write(data[pos:end])
		pos += n
		if err != nil {
			return
		}
		if cancelAfter >= 0 && pos >= cancelAfter {
			cancel()
			// Keep reading context state through the next write so the
			// pipe close is driven by the same cancellation, never by a
			// different error.
			<-ctx.Done()
			pc.finish(func() error { return ctx.Err() })
			return
		}
		if pace > 0 {
			time.Sleep(pace)
		}
	}

	// All bytes were delivered before cancellation: close cleanly with EOF
	// so a parser that has already drained the pipe completes successfully,
	// mirroring a real request whose body finished just before cancel.
	if err := ctx.Err(); err != nil {
		pc.finish(func() error { return err })
		return
	}
	pc.finish(func() error { return io.EOF })
}

// Reader is the side Parse must consume.
func (pc *PipeCancel) Reader() io.Reader { return pc.pr }

// Wait blocks until the producer goroutine has exited and the write side of
// the pipe is closed, or until timeout elapses. A non-nil result means the
// transport leaked a goroutine after cancellation/error.
func (pc *PipeCancel) Wait(timeout time.Duration) error {
	select {
	case <-pc.done:
		return nil
	case <-time.After(timeout):
		return errProducerLeak{}
	}
}

// Closed reports whether the write side was closed exactly once.
func (pc *PipeCancel) Closed() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.closed
}

// ReadClosedAfterWait verifies from the reader side that the pipe is closed:
// after Wait succeeds, any further read must return an error rather than
// blocking.
func (pc *PipeCancel) ReadClosedAfterWait() error {
	done := make(chan error, 1)
	go func() {
		_, err := pc.pr.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			return errPipeNotClosed{}
		}
		return nil
	case <-time.After(2 * time.Second):
		return errPipeNotClosed{}
	}
}

func (pc *PipeCancel) finish(cause func() error) {
	pc.doneOnce.Do(func() {
		err := cause()
		pc.mu.Lock()
		pc.closed = true
		pc.closeErr = err
		pc.mu.Unlock()
		_ = pc.pw.CloseWithError(err)
		close(pc.done)
	})
}
