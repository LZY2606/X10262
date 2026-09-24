package formstream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"bytes"
)

var (
	// ErrInvalidBoundary is returned when the boundary does not comply with
	// the character and length restrictions defined by RFC 2046.
	ErrInvalidBoundary = errors.New("formstream: invalid boundary")
	// ErrBodyTooLarge is returned when the generated body exceeds the
	// configured maximum size.
	ErrBodyTooLarge = errors.New("formstream: body too large")
	// ErrStreamClosed is returned when a closed or failed StreamWriter is used.
	ErrStreamClosed = errors.New("formstream: stream writer already closed")
)

// PartWriter is the body of a single multipart part.
//
// Writes are streamed to the destination writer through a goroutine and an
// io.Pipe: blocking writes apply back-pressure to the producer, and failures
// of the destination, of the context or of the StreamWriter are surfaced as
// write errors. Close marks the part as finished; CloseWithError aborts it
// without sending a terminating boundary, and the supplied error becomes the
// error reported by StreamWriter.Close.
type PartWriter interface {
	io.Writer
	Close() error
	CloseWithError(error) error
}

// StreamWriterOption configures a StreamWriter.
type StreamWriterOption func(*streamWriterConfig)

type streamWriterConfig struct {
	maxParts      uint
	maxPartsLimit bool
	maxBodySize int64
}

// WithWriterMaxParts limits the number of parts a StreamWriter can generate.
// A value of 0 disables the limit.
func WithWriterMaxParts(maxParts uint) StreamWriterOption {
	return func(c *streamWriterConfig) {
		c.maxPartsLimit = true
		c.maxParts = maxParts
	}
}

// WithMaxBodySize limits the total number of content bytes (excluding
// boundaries and headers) a StreamWriter can generate. A value of 0
// disables the limit.
func WithMaxBodySize(maxBodySize int64) StreamWriterOption {
	return func(c *streamWriterConfig) {
		c.maxBodySize = maxBodySize
	}
}

// StreamWriter generates multipart/form-data bodies and streams them to an
// io.Writer without buffering a whole part.
//
// The wire format is byte-for-byte identical to mime/multipart.Writer:
// boundaries are CRLF terminated, headers are emitted in canonical
// textproto order and the body ends with a closing boundary. Generation runs
// on a single pump goroutine fed through io.Pipes, so a slow destination
// applies back-pressure, ctx cancellation aborts the transfer, and a source
// reader or destination failure is reported exactly once, by the failing
// operation and then by Close.
type StreamWriter struct {
	ctx      context.Context
	dst      io.Writer
	boundary string
	cfg      streamWriterConfig

	mu       sync.Mutex
	started  bool
	closed   bool
	terminal error
	current  *io.PipeWriter
	jobs     chan partJob
	done     chan struct{}
	doneOnce sync.Once
	jobsOnce sync.Once
}

type partJob struct {
	pr     *io.PipeReader
	header textproto.MIMEHeader
}

// NewStreamWriter returns a StreamWriter writing multipart/form-data bodies
// to w. The transfer lifetime is governed by ctx; every write to a part is
// interrupted as soon as ctx is done. The boundary is generated randomly and
// can be inspected with Boundary.
func NewStreamWriter(ctx context.Context, w io.Writer, options ...StreamWriterOption) *StreamWriter {
	cfg := streamWriterConfig{}
	for _, opt := range options {
		opt(&cfg)
	}

	return &StreamWriter{
		ctx:      ctx,
		dst:      w,
		boundary: randomStreamBoundary(),
		cfg:      cfg,
		done:     make(chan struct{}),
	}
}

func randomStreamBoundary() string {
	var buf [30]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", buf[:])
}

// Boundary returns the boundary of the multipart message.
func (sw *StreamWriter) Boundary() string {
	return sw.boundary
}

// FormDataContentType returns the Content-Type for an HTTP
// multipart/form-data request generated with this StreamWriter.
func (sw *StreamWriter) FormDataContentType() string {
	b := sw.boundary
	if strings.ContainsAny(b, `()<>@,;:\"/[]?= `) {
		b = `"` + b + `"`
	}

	return "multipart/form-data; boundary=" + b
}

// SetBoundary overrides the randomly generated boundary. It must be called
// before the first part is created and the boundary must be non-empty, at
// most 70 bytes and only contain RFC 2046 bchars.
func (sw *StreamWriter) SetBoundary(boundary string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.started {
		return fmt.Errorf("formstream: boundary cannot be changed after write: %w", ErrInvalidBoundary)
	}

	if err := validateBoundary(boundary); err != nil {
		return err
	}

	sw.boundary = boundary

	return nil
}

func validateBoundary(boundary string) error {
	// rfc2046#section-5.1.1
	if len(boundary) < 1 || len(boundary) > 70 {
		return fmt.Errorf("formstream: invalid boundary length: %w", ErrInvalidBoundary)
	}

	end := len(boundary) - 1
	for i, b := range boundary {
		if 'A' <= b && b <= 'Z' || 'a' <= b && b <= 'z' || '0' <= b && b <= '9' {
			continue
		}

		switch b {
		case '\'', '(', ')', '+', '_', ',', '-', '.', '/', ':', '=', '?':
			continue
		case ' ':
			if i != end {
				continue
			}
		}

		return fmt.Errorf("formstream: invalid boundary character %q: %w", b, ErrInvalidBoundary)
	}

	return nil
}

// CreatePart creates a new multipart section with the provided header and
// returns its body writer. Any previous part is closed first. The caller must
// close the returned part; StreamWriter.Close closes it automatically.
func (sw *StreamWriter) CreatePart(header textproto.MIMEHeader) (PartWriter, error) {
	if header == nil {
		header = textproto.MIMEHeader{}
	}

	sw.mu.Lock()
	if sw.closed {
		err := sw.terminal
		if err == nil {
			err = ErrStreamClosed
		}
		sw.mu.Unlock()

		return nil, err
	}

	prev := sw.current
	if sw.cfg.maxPartsLimit && sw.cfg.maxParts == 0 {
		sw.mu.Unlock()

		return nil, ErrTooManyParts
	}
	sw.cfg.maxParts--

	pr, pw := io.Pipe()
	if !sw.started {
		sw.started = true
		sw.jobs = make(chan partJob)
		go sw.pump(sw.jobs)
		if sw.ctx != nil {
			go sw.watchCtx(sw.jobs)
		}
	}
	jobs := sw.jobs
	sw.current = pw
	sw.mu.Unlock()

	// Closing the previous part can fail when the transfer already aborted;
	// in that case the same terminal error is about to surface here.
	if prev != nil {
		if closeErr := prev.Close(); closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
			return nil, closeErr
		}
	}

	job := partJob{pr: pr, header: cloneMIMEHeader(header)}
	select {
	case jobs <- job:
	case <-sw.done:
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.terminal != nil {
		// The pump ended before accepting the job: release the still-open
		// pipe so the caller's later writes fail instead of deadlocking.
		_ = pw.CloseWithError(sw.terminal)
		return nil, sw.terminal
	}

	return pw, nil
}

func cloneMIMEHeader(h textproto.MIMEHeader) textproto.MIMEHeader {
	clone := make(textproto.MIMEHeader, len(h))
	maps.Copy(clone, h)

	return clone
}

// CreateFormField creates a new part with a form-data Content-Disposition
// header for the given field name.
func (sw *StreamWriter) CreateFormField(fieldName string) (PartWriter, error) {
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"`, escapeStreamQuotes(fieldName)))

	return sw.CreatePart(header)
}

// CreateFormFile creates a new part with the given field name, file name and
// an application/octet-stream Content-Type.
func (sw *StreamWriter) CreateFormFile(fieldName, fileName string) (PartWriter, error) {
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
		escapeStreamQuotes(fieldName), escapeStreamQuotes(fileName)))
	header.Set("Content-Type", "application/octet-stream")

	return sw.CreatePart(header)
}

var streamQuoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

func escapeStreamQuotes(s string) string {
	return streamQuoteEscaper.Replace(s)
}

// WriteField creates a field part and writes value to it.
func (sw *StreamWriter) WriteField(fieldName, value string) error {
	pw, err := sw.CreateFormField(fieldName)
	if err != nil {
		return err
	}

	if _, err := io.WriteString(pw, value); err != nil {
		_ = pw.CloseWithError(err)
		return sw.waitTerminal()
	}

	return pw.Close()
}

// Close finishes the multipart message by writing the closing boundary. It
// returns the transfer error if the destination, a source reader or the
// context failed; the same error is returned on every later call.
func (sw *StreamWriter) Close() error {
	sw.mu.Lock()
	if sw.ctx != nil {
		if err := sw.ctx.Err(); err != nil {
			sw.mu.Unlock()
			sw.fail(err)

			return sw.waitTerminal()
		}
	}

	prev := sw.current
	if !sw.started {
		sw.closed = true
		sw.mu.Unlock()

		if err := writeAll(sw.dst, []byte(fmt.Sprintf("\r\n--%s--\r\n", sw.boundary))); err != nil {
			sw.fail(err)

			return err
		}
		sw.finish(nil)

		return nil
	}

	jobs := sw.jobs
	sw.current = nil
	sw.mu.Unlock()

	if prev != nil {
		_ = prev.Close()
	}
	sw.jobsOnce.Do(func() { close(jobs) })

	return sw.waitTerminal()
}

func (sw *StreamWriter) waitTerminal() error {
	<-sw.done

	sw.mu.Lock()
	err := sw.terminal
	sw.mu.Unlock()

	return err
}

// fail aborts the transfer with err. The current part (blocked in a Write),
// the part being drained by the pump and any idle pump waiting for the next
// part are released so every goroutine and pipe exits.
func (sw *StreamWriter) fail(err error) {
	sw.mu.Lock()
	if sw.terminal != nil || sw.closed {
		sw.mu.Unlock()

		return
	}
	sw.terminal = err

	current := sw.current
	var jobs chan partJob
	if sw.started {
		jobs = sw.jobs
	}
	sw.mu.Unlock()

	if current != nil {
		_ = current.CloseWithError(err)
	}
	if jobs != nil {
		sw.jobsOnce.Do(func() { close(jobs) })
	}

	<-sw.done
}

// finish records the successful (or failed, set by the pump) terminal state.
func (sw *StreamWriter) finish(err error) {
	sw.mu.Lock()
	sw.closed = true
	if err != nil {
		sw.terminal = err
	}
	sw.mu.Unlock()
	sw.doneOnce.Do(func() { close(sw.done) })
}

// pump is the single goroutine that owns the destination and serializes all
// framing and part content. Production paths (CreatePart, WriteField,
// WriteFile, Close) all converge on this pump; there is no alternate
// implementation for tests.
func (sw *StreamWriter) pump(jobs <-chan partJob) {
	defer func() { sw.finish(sw.terminal) }()

	var remaining int64
	if sw.cfg.maxBodySize > 0 {
		remaining = sw.cfg.maxBodySize
	}

	buf, ok := streamBufPool.Get().(*[]byte)
	if !ok || buf == nil {
		b := make([]byte, 32*1024)
		buf = &b
	}
	defer streamBufPool.Put(buf)

	first := true
	for {
		var job partJob
		select {
		case j, ok := <-jobs:
			if !ok {
				sw.pumpClose(remaining)
				return
			}
			job = j
		case <-sw.ctxDone():
			sw.pumpAbort(sw.ctx.Err(), nil)
			return
		}

		if first {
			first = false
			if err := writeAll(sw.dst, []byte(fmt.Sprintf("--%s\r\n", sw.boundary))); err != nil {
				sw.pumpAbort(err, job.pr)
				return
			}
		} else {
			if err := writeAll(sw.dst, []byte(fmt.Sprintf("\r\n--%s\r\n", sw.boundary))); err != nil {
				sw.pumpAbort(err, job.pr)
				return
			}
		}

		var headerBuf bytes.Buffer
		for _, key := range slices.Sorted(maps.Keys(job.header)) {
			for _, value := range job.header[key] {
				fmt.Fprintf(&headerBuf, "%s: %s\r\n", key, value)
			}
		}
		headerBuf.WriteString("\r\n")
		if err := writeAll(sw.dst, headerBuf.Bytes()); err != nil {
			sw.pumpAbort(err, job.pr)
			return
		}

		nextRemaining, err := sw.copyPart(job.pr, remaining, *buf)
		_ = job.pr.Close()
		if err != nil {
			sw.pumpAbort(err, nil)
			return
		}
		remaining = nextRemaining
	}
}

func (sw *StreamWriter) ctxDone() <-chan struct{} {
	sw.mu.Lock()
	ctx := sw.ctx
	sw.mu.Unlock()
	if ctx == nil {
		return nil
	}

	return ctx.Done()
}

func (sw *StreamWriter) pumpClose(_ int64) {
	if err := writeAll(sw.dst, []byte(fmt.Sprintf("\r\n--%s--\r\n", sw.boundary))); err != nil {
		sw.pumpAbort(err, nil)

		return
	}

	sw.finish(nil)
}

// pumpAbort records the first terminal error and releases the pipe still
// held by the failed part, unblocking its producer.
func (sw *StreamWriter) pumpAbort(err error, pr *io.PipeReader) {
	if pr != nil {
		_ = pr.CloseWithError(err)
	}

	sw.mu.Lock()
	current := sw.current
	if sw.terminal == nil {
		sw.terminal = err
	}
	err = sw.terminal
	sw.mu.Unlock()

	if current != nil {
		_ = current.CloseWithError(err)
	}
	sw.jobsOnce.Do(func() {
		if sw.jobs != nil {
			close(sw.jobs)
		}
	})
}

// copyPart streams one part body into the destination while enforcing the
// total content-byte budget. Content accepted so far always reaches the
// destination before the budget error is raised.
func (sw *StreamWriter) copyPart(pr io.Reader, remaining int64, buf []byte) (int64, error) {
	for {
		if sw.cfg.maxBodySize > 0 && remaining == 0 {
			// The whole budget was consumed: one further byte decides between
			// a part ending exactly at the limit and an overflow.
			_, probeErr := pr.Read(buf[:1])
			if probeErr == nil {
				return remaining, ErrBodyTooLarge
			}

			if errors.Is(probeErr, io.EOF) {
				return remaining, nil
			}

			return remaining, probeErr
		}

		readBuf := buf
		if sw.cfg.maxBodySize > 0 && int64(len(readBuf)) > remaining {
			readBuf = buf[:remaining]
		}

		n, readErr := pr.Read(readBuf)
		if n > 0 {
			if err := writeAll(sw.dst, buf[:n]); err != nil {
				return remaining, err
			}
			remaining -= int64(n)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return remaining, nil
			}

			return remaining, readErr
		}
	}
}

// watchCtx aborts the transfer as soon as the context is done, including
// while the pump is idle between parts.
func (sw *StreamWriter) watchCtx(jobs <-chan partJob) {
	select {
	case <-sw.done:
	case <-sw.ctx.Done():
		sw.fail(sw.ctx.Err())
	}
}

// writeAll writes the whole payload and turns a contract-violating short
// write without an error into io.ErrShortWrite, the same sentinel used by the
// standard library transport helpers.
func writeAll(w io.Writer, payload []byte) error {
	n, err := w.Write(payload)
	if err != nil {
		return err
	}

	if n < len(payload) {
		return io.ErrShortWrite
	}

	return nil
}

var streamBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)

		return &b
	},
}

// WriteFile creates a file part with the given content type (application/
// octet-stream when empty) and streams r into it until EOF. A read failure
// aborts the whole transfer and is returned.
func (sw *StreamWriter) WriteFile(fieldName, fileName, contentType string, r io.Reader) error {
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
		escapeStreamQuotes(fieldName), escapeStreamQuotes(fileName)))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	header.Set("Content-Type", contentType)

	pw, err := sw.CreatePart(header)
	if err != nil {
		return err
	}

	if _, err := io.Copy(pw, r); err != nil {
		_ = pw.CloseWithError(err)
		return sw.waitTerminal()
	}

	return pw.Close()
}
