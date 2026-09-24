package formstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// errPipeShutdown unblocks the pump goroutine when parsing ends without
// cancellation. It is an internal sentinel and never escapes this package.
var errPipeShutdown = errors.New("formstream: parser stopped reading")

// liveCancelableReaders counts cancelableReader instances whose goroutines
// have not exited yet. It is observed by tests (via export_test.go) to prove
// that cancellation leaves no goroutine or pipe behind.
var liveCancelableReaders atomic.Int64

// cancelableReader makes reads from an arbitrary io.Reader abortable via
// context cancellation. A pump goroutine copies src into an io.Pipe and a
// watcher closes the pipe with the context error once ctx is done, so a
// blocked Read on the pipe unblocks promptly. done is closed after all
// goroutines have exited.
//
// The pump can only exit as promptly as src allows: a Read blocked inside
// src itself returns when src observes the cancellation (e.g. a request
// body tied to the request context) or when src reaches EOF.
type cancelableReader struct {
	pr       *io.PipeReader
	pw       *io.PipeWriter
	stop     chan struct{}
	done     chan struct{}
	closeErr sync.Once
	closeStp sync.Once
}

func newCancelableReader(ctx context.Context, src io.Reader) *cancelableReader {
	pr, pw := io.Pipe()
	cr := &cancelableReader{
		pr:   pr,
		pw:   pw,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	liveCancelableReaders.Add(1)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(pw, src)
		cr.closeWithErr(io.EOF)
	}()

	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			cr.closeWithErr(ctx.Err())
		case <-cr.stop:
		}
	}()

	go func() {
		wg.Wait()
		close(cr.done)
		liveCancelableReaders.Add(-1)
	}()

	return cr
}

func (cr *cancelableReader) Read(p []byte) (int, error) {
	return cr.pr.Read(p)
}

// Close unblocks the pump goroutine and releases the watcher. It is
// idempotent and never reports an error to the caller.
func (cr *cancelableReader) Close() error {
	cr.closeWithErr(errPipeShutdown)
	cr.closeStp.Do(func() {
		close(cr.stop)
	})

	return nil
}

// closeWithErr closes the pipe write side exactly once; the first error
// wins, so a cancellation is reported at most once.
func (cr *cancelableReader) closeWithErr(err error) {
	cr.closeErr.Do(func() {
		_ = cr.pw.CloseWithError(err)
	})
}
