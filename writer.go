package formstream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

var (
	// ErrInvalidBoundary is returned when the configured multipart boundary
	// does not satisfy the multipart/form-data grammar.
	ErrInvalidBoundary = errors.New("invalid multipart boundary")
	// ErrInvalidPart is returned when a part cannot be encoded because its
	// name, filename or content source is missing.
	ErrInvalidPart = errors.New("invalid multipart part")
)

// WriterOption configures multipart body generation.
type WriterOption func(*writerConfig)

type writerConfig struct {
	boundary string
	// maxParts limits the number of parts encoded in one body.
	// A zero value means no limit.
	maxParts uint
}

// WithBoundary pins the multipart boundary instead of generating a random one.
// It follows the same grammar rules as mime/multipart.Writer.SetBoundary.
func WithBoundary(boundary string) WriterOption {
	return func(c *writerConfig) {
		c.boundary = boundary
	}
}

// WithWriterMaxParts caps the number of parts accepted by a single generation
// call. When more parts are supplied, generation fails with ErrTooManyParts
// before any byte is written.
func WithWriterMaxParts(maxParts uint) WriterOption {
	return func(c *writerConfig) {
		c.maxParts = maxParts
	}
}

// Part is one multipart/form-data section. Instances must be created with
// FieldPart or FilePart so that callers cannot depend on internal layout.
type Part struct {
	name        string
	filename    string
	contentType string
	content     io.Reader
}

// PartOption customizes a part.
type PartOption func(*Part)

// WithContentType overrides the Content-Type header of a part. File parts
// default to application/octet-stream; field parts omit the header.
func WithContentType(contentType string) PartOption {
	return func(p *Part) {
		p.contentType = contentType
	}
}

// FieldPart creates a regular form field.
func FieldPart(name, value string) Part {
	return Part{
		name:    name,
		content: strings.NewReader(value),
	}
}

// FilePart creates a file upload part.
func FilePart(name, filename string, content io.Reader, options ...PartOption) Part {
	p := Part{
		name:        name,
		filename:    filename,
		contentType: "application/octet-stream",
		content:     content,
	}
	for _, opt := range options {
		opt(&p)
	}

	return p
}

// WriteParts generates a multipart/form-data body into w.
//
// The context is honored between framing and content writes: cancellation is
// reported with the context's own error. A target writer that returns a short
// write without an error makes the call fail with io.ErrShortWrite, matching
// the io.Writer contract. On any non-nil error the bytes already sent to w are
// an incomplete body and must be discarded.
//
// The returned boundary is the one used on the wire, generated randomly when
// WithBoundary was not supplied.
func WriteParts(ctx context.Context, w io.Writer, parts []Part, options ...WriterOption) (string, error) {
	if w == nil {
		return "", fmt.Errorf("%w: nil destination writer", ErrInvalidPart)
	}

	cfg, err := newWriterConfig(options)
	if err != nil {
		return "", err
	}

	if err := prepareParts(parts, cfg); err != nil {
		return cfg.boundary, err
	}

	if err := ctx.Err(); err != nil {
		return cfg.boundary, err
	}

	if err := writeParts(ctx, w, parts, cfg); err != nil {
		return cfg.boundary, err
	}

	return cfg.boundary, nil
}

// StreamParts generates the same body as WriteParts, but returns a reader
// backed by an io.Pipe so production can start before every source part has
// been buffered. The second return value is the HTTP Content-Type value.
//
// Both entry points share writeParts; StreamParts only adds pipe transport.
// Configuration and input are validated before the goroutine starts, so
// rejection errors are returned here directly and are repeatable.
//
// A source reader error or a target-side short write cannot occur on the pipe
// itself (pipe reads may legitimately return partial data); exercise those
// causes through WriteParts. Cancellation unblocks a producer waiting on a
// slow consumer and terminates the body with the context's error. Callers must
// close the returned body to release the goroutine; a source blocked forever
// inside its own Read cannot be interrupted, mirroring io and mime/multipart.
func StreamParts(ctx context.Context, parts []Part, options ...WriterOption) (io.ReadCloser, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg, err := newWriterConfig(options)
	if err != nil {
		return nil, "", err
	}

	if err := prepareParts(parts, cfg); err != nil {
		return nil, "", err
	}

	contentType := formDataContentType(cfg.boundary)
	pipeReader, pipeWriter := io.Pipe()
	gctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		genErr := writeParts(gctx, pipeWriter, parts, cfg)
		_ = pipeWriter.CloseWithError(genErr)
	}()

	// Close the read side when cancellation wins so a producer blocked on
	// pipe backpressure exits instead of leaking a goroutine. The done
	// channel is preferred: a fully produced body must surface io.EOF even
	// if the parent context ended racing the final write.
	go func() {
		select {
		case <-done:
			return
		default:
		}

		select {
		case <-done:
		case <-gctx.Done():
			_ = pipeReader.CloseWithError(gctx.Err())
		}
	}()

	body := &streamBody{
		reader: pipeReader,
		cancel: cancel,
	}

	return body, contentType, nil
}

type streamBody struct {
	reader    *io.PipeReader
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (b *streamBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *streamBody) Close() error {
	b.closeOnce.Do(func() {
		b.cancel()
		_ = b.reader.Close()
	})

	return nil
}

func newWriterConfig(options []WriterOption) (*writerConfig, error) {
	cfg := &writerConfig{}
	for _, opt := range options {
		opt(cfg)
	}

	if cfg.boundary == "" {
		cfg.boundary = randomBoundary()
	}
	if !validBoundary(cfg.boundary) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidBoundary, cfg.boundary)
	}

	return cfg, nil
}

func prepareParts(parts []Part, cfg *writerConfig) error {
	if cfg.maxParts != 0 && uint(len(parts)) > cfg.maxParts {
		return fmt.Errorf("%w: %d parts exceed limit %d", ErrTooManyParts, len(parts), cfg.maxParts)
	}

	for i := range parts {
		p := &parts[i]
		if p.name == "" {
			return fmt.Errorf("%w: part %d has empty name", ErrInvalidPart, i)
		}
		if p.content == nil {
			return fmt.Errorf("%w: part %d (%q) has nil content", ErrInvalidPart, i, p.name)
		}
	}

	return nil
}

// writeParts is the single framing algorithm shared by WriteParts and
// StreamParts. The wire layout is identical to mime/multipart.Writer:
//
//	--boundary\r\n
//	Content-Disposition: form-data; name="..."[; filename="..."]\r\n
//	[Content-Type: ...\r\n]
//	\r\n
//	content
//	\r\n--boundary--\r\n
func writeParts(ctx context.Context, w io.Writer, parts []Part, cfg *writerConfig) error {
	if len(parts) == 0 {
		return writeFull(ctx, w, []byte("--"+cfg.boundary+"--\r\n"))
	}

	delimStart := "--"
	for i := range parts {
		p := &parts[i]

		if err := writeFull(ctx, w, []byte(delimStart+cfg.boundary+"\r\n")); err != nil {
			return fmt.Errorf("write boundary of part %d (%q): %w", i, p.name, err)
		}

		head := strings.Builder{}
		head.WriteString("Content-Disposition: form-data; name=\"")
		head.WriteString(escapePartValue(p.name))
		head.WriteString("\"")
		if p.filename != "" {
			head.WriteString("; filename=\"")
			head.WriteString(escapePartValue(p.filename))
			head.WriteString("\"")
		}
		head.WriteString("\r\n")
		if p.contentType != "" {
			head.WriteString("Content-Type: ")
			head.WriteString(p.contentType)
			head.WriteString("\r\n")
		}
		head.WriteString("\r\n")
		if err := writeFull(ctx, w, []byte(head.String())); err != nil {
			return fmt.Errorf("write headers of part %d (%q): %w", i, p.name, err)
		}

		if err := copyPartContent(ctx, w, p.content); err != nil {
			return fmt.Errorf("copy content of part %d (%q): %w", i, p.name, err)
		}

		delimStart = "\r\n--"
	}

	if err := writeFull(ctx, w, []byte(delimStart+cfg.boundary+"--\r\n")); err != nil {
		return fmt.Errorf("write closing boundary: %w", err)
	}

	return nil
}

func writeFull(ctx context.Context, w io.Writer, p []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p) == 0 {
		return nil
	}

	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return fmt.Errorf("%w: wrote %d of %d bytes", io.ErrShortWrite, n, len(p))
	}

	return nil
}

func copyPartContent(ctx context.Context, w io.Writer, r io.Reader) (err error) {
	buf, ok := writerBufPool.Get().(*[]byte)
	if !ok || buf == nil {
		b := make([]byte, 32*1024)
		buf = &b
	}
	defer writerBufPool.Put(buf)

	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		var readN int
		readN, err = r.Read((*buf)[:cap(*buf)])
		if readN > 0 {
			if writeErr := writeFull(ctx, w, (*buf)[:readN]); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

var writerBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

// escapePartValue matches mime/multipart.escapeQuotes: backslash and quote are
// backslash escaped, CR/LF are percent encoded.
func escapePartValue(s string) string {
	return partValueEscaper.Replace(s)
}

var partValueEscaper = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	"\r", "%0D",
	"\n", "%0A",
)

func randomBoundary() string {
	var buf [30]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf[:])
}

// validBoundary follows the grammar enforced by mime/multipart.Writer.SetBoundary.
func validBoundary(boundary string) bool {
	if len(boundary) < 1 || len(boundary) > 70 {
		return false
	}

	end := len(boundary) - 1
	for i, b := range boundary {
		switch {
		case 'A' <= b && b <= 'Z', 'a' <= b && b <= 'z', '0' <= b && b <= '9':
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

		return false
	}

	return true
}

func formDataContentType(boundary string) string {
	if strings.ContainsAny(boundary, `()<>@,;:\"/[]?= `) {
		boundary = `"` + boundary + `"`
	}
	return "multipart/form-data; boundary=" + boundary
}
