package formstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"sync"

	conditionjudge "github.com/mazrean/formstream/internal/condition_judge"
)

var (
	// ErrTooManyParts is returned when the parts are more than MaxParts.
	ErrTooManyParts = errors.New("too many parts")
	// ErrTooManyHeaders is returned when the headers are more than MaxHeaders.
	ErrTooManyHeaders = errors.New("too many headers")
	// ErrTooLargeForm is returned when the form is too large for the parser to handle within the memory limit.
	ErrTooLargeForm = errors.New("too large form")
)

// Parse parses the multipart form from r.
func (p *Parser) Parse(r io.Reader) (err error) {
	return p.ParseContext(context.Background(), r)
}

// ParseContext behaves like Parse but additionally honors ctx cancellation.
// It runs the exact same parsing algorithm as Parse; the only added machinery
// is a pipe transport between the source reader and the parser. On
// cancellation both pipe ends are closed with the context cause, releasing any
// blocked read inside a hook and any blocked pump write at once. The failure
// is reported exactly once: the result is either the parsing/hook/source
// error, or the context cause when ctx is canceled.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}

	// Without a cancellation channel the pipe would only add a goroutine and
	// copies, so keep the original direct-read path byte-identical.
	if ctx.Done() == nil {
		return p.parseWithHooks(r)
	}

	pr, pw := io.Pipe()

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		_, copyErr := io.Copy(pw, r)
		// Closing the write end also unblocks the parser when it has already
		// closed the read end because of a parsing error.
		pw.CloseWithError(copyErr)
	}()

	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			// Close both ends with the same cause: a blocked parser/hook read
			// is released via pr, a blocked pump write via pw.
			cause := context.Cause(ctx)
			pr.CloseWithError(cause)
			pw.CloseWithError(cause)
		case <-stopWatch:
		}
	}()

	parseErr := p.parseWithHooks(pr)
	close(stopWatch)

	select {
	case <-ctx.Done():
		// Cancellation owns the reported error.
		parseErr = context.Cause(ctx)
		pr.CloseWithError(parseErr)
	default:
		pr.CloseWithError(parseErr)
	}
	<-pumpDone
	<-watchDone

	return parseErr
}

func (p *Parser) parseWithHooks(r io.Reader) (err error) {
	hsc := newHookSatisfactionChecker(p.hookMap, &p.parserConfig)
	defer func() {
		deferErr := hsc.Close()
		// capture the error of Close()
		if deferErr != nil {
			if err != nil {
				err = errors.Join(err, deferErr)
			} else {
				err = deferErr
			}
		}
	}()

	err = p.parse(r, hsc.IConditionJudger)

	return
}

func (p *Parser) parse(r io.Reader, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
	mr := multipart.NewReader(r, p.boundary)
	for {
		var part *multipart.Part
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next part: %w", err)
		}

		if p.maxParts == 0 {
			return ErrTooManyParts
		}
		p.maxParts--

		for _, header := range part.Header {
			if p.maxHeaders < uint(len(header)) {
				return ErrTooManyHeaders
			}
			p.maxHeaders -= uint(len(header))
		}

		header := newHeader(part.Header)
		if hsc.IsHookExist(part.FormName()) {
			_, err := hsc.HookEvent(part.FormName(), &normalParam{
				r: part,
				h: header,
			})
			if err != nil {
				return fmt.Errorf("failed to run or set hook: %w", err)
			}
		} else {
			b := new(bytes.Buffer)

			if DataSize(len(part.FormName())) > p.maxMemSize {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(len(part.FormName()))

			n, err := io.Copy(b, part)
			if err != nil {
				return fmt.Errorf("failed to copy part: %w", err)
			}

			if uint64(n) > uint64(p.maxMemSize) {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(n)

			p.valueMap[part.FormName()] = append(p.valueMap[part.FormName()], Value{
				content: b.Bytes(),
				header:  header,
			})
		}

		err = hsc.KeyEvent(part.FormName())
		if err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	}

	return nil
}

type hookSatisfactionChecker struct {
	conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]
	preProcessor *preProcessor
}

func newHookSatisfactionChecker(streamHooks map[string]streamHook, config *parserConfig) *hookSatisfactionChecker {
	judgeHooks := make(map[string]conditionjudge.Hook[string, *normalParam, *abnormalParam], len(streamHooks))
	for name, hook := range streamHooks {
		h := judgeHook(hook)
		judgeHooks[name] = &h
	}

	preProcess := &preProcessor{
		config: config,
		pool:   defaultBufferPool,
	}

	return &hookSatisfactionChecker{
		IConditionJudger: conditionjudge.NewConditionJudger(judgeHooks, preProcess.run),
		preProcessor:     preProcess,
	}
}

func (wh *hookSatisfactionChecker) Close() error {
	return wh.preProcessor.Close()
}

type normalParam struct {
	r io.Reader
	h Header
}

type abnormalParam struct {
	content io.ReadCloser
	header  Header
}

type preProcessor struct {
	config *parserConfig
	offset int64
	file   *os.File
	// pool buffers the in-memory contents of parts whose hooks are still
	// waiting for their required parts. Production parsers always use
	// defaultBufferPool. It is a field rather than a package global solely so
	// internal tests can observe get/put balance while exercising this exact
	// same run path; no parsing logic differs between implementations.
	pool bufferPool
}

// bufferPool is the minimal seam used by preProcessor. The production
// implementation is backed by a sync.Pool; tests may substitute a counting
// implementation without duplicating any parsing logic.
type bufferPool interface {
	Get() *bytes.Buffer
	Put(*bytes.Buffer)
}

type syncBufferPool struct{ p sync.Pool }

var defaultBufferPool bufferPool = &syncBufferPool{
	p: sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
	},
}

func (sp *syncBufferPool) Get() *bytes.Buffer {
	buf, ok := sp.p.Get().(*bytes.Buffer)
	if !ok {
		return new(bytes.Buffer)
	}
	return buf
}

func (sp *syncBufferPool) Put(buf *bytes.Buffer) {
	sp.p.Put(buf)
}

func (pp *preProcessor) run(normalParam *normalParam) (*abnormalParam, error) {
	buf := pp.pool.Get()
	buf.Reset()

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, normalParam.r, int64(memLimit)+1)
	if err != nil && !errors.Is(err, io.EOF) {
		pp.pool.Put(buf)
		return nil, fmt.Errorf("failed to copy: %w", err)
	}

	var content io.ReadCloser
	if DataSize(n) > memLimit {
		if pp.file == nil {
			f, err := os.CreateTemp("", "formstream-")
			if err != nil {
				pp.pool.Put(buf)
				return nil, fmt.Errorf("failed to create temp file: %w", err)
			}
			pp.file = f
		}

		bufSize, err := io.Copy(pp.file, buf)
		pp.pool.Put(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to write: %w", err)
		}

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			return nil, fmt.Errorf("failed to copy: %w", err)
		}

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size

	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()
		content = customReadCloser{
			Reader: buf,
			closeFunc: func() error {
				pp.pool.Put(buf)
				pp.config.maxMemSize += DataSize(bufSize)
				pp.config.maxMemFileSize += DataSize(bufSize)
				return nil
			},
		}
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	if pp.file == nil {
		return nil
	}

	filepath := pp.file.Name()

	// Close the file handle first
	closeErr := pp.file.Close()

	// Remove the temporary file from disk
	removeErr := os.Remove(filepath)

	// Return combined errors if any
	if closeErr != nil || removeErr != nil {
		return errors.Join(closeErr, removeErr)
	}

	return nil
}

type judgeHook struct {
	fn           StreamHookFunc
	requireParts []string
}

func (jh judgeHook) NormalPath(normalParam *normalParam) error {
	return jh.fn(normalParam.r, normalParam.h)
}

func (jh judgeHook) AbnormalPath(abnoramlParam *abnormalParam) error {
	defer abnoramlParam.content.Close()

	return jh.fn(abnoramlParam.content, abnoramlParam.header)
}

func (jh judgeHook) Requirements() []string {
	return jh.requireParts
}

type customReadCloser struct {
	io.Reader
	closeFunc func() error
}

func (cc customReadCloser) Close() error {
	return cc.closeFunc()
}
