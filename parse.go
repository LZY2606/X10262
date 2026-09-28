package formstream

import (
	"bytes"
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
	// Parsing mutates resource counters on parserConfig (maxParts, maxHeaders
	// and maxMemSize). Work on a private copy so that a failed parse cannot
	// half-update the parser's configured budgets and the same parser can be
	// retried with identical results.
	config := p.parserConfig
	hsc := newHookSatisfactionChecker(p.hookMap, &config)
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

	// Only commit parsed values after the whole form succeeds, so that an
	// aborted parse leaves neither fields nor counters in a half-updated
	// state. Parsing writes into a private copy; the parser's valueMap is
	// restored to its pre-parse snapshot on failure.
	rollback := p.valueMap
	working := make(map[string][]Value, len(rollback))
	for key, values := range rollback {
		working[key] = append([]Value(nil), values...)
	}
	p.valueMap = working
	defer func() {
		if err != nil {
			p.valueMap = rollback
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

		err = p.processPart(part, hsc)
		if err != nil {
			// Drain the remainder of the current part. When the source is a
			// streaming producer (e.g. io.Pipe), an error path that leaves an
			// unconsumed part would block the producer forever and leak the
			// goroutine and pipe. multipart.Reader drains here itself when
			// NextPart advances, but aborted parses never advance again.
			_, _ = io.Copy(io.Discard, part)
			return err
		}
	}

	return nil
}

func (p *Parser) processPart(part *multipart.Part, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
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

	err := hsc.KeyEvent(part.FormName())
	if err != nil {
		return fmt.Errorf("failed to run satisfied hook: %w", err)
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
	// pending holds in-memory contents buffered for hooks whose required
	// parts have not arrived yet. They must be returned to the pool (and
	// their memory budget restored) when the parse ends before the hook ran,
	// otherwise a failed parse leaks pool buffers and budget.
	pending []customReadCloser
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (pp *preProcessor) run(normalParam *normalParam) (*abnormalParam, error) {
	buf, ok := bufPool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}
	buf.Reset()

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, normalParam.r, int64(memLimit)+1)
	if err != nil && !errors.Is(err, io.EOF) {
		bufPool.Put(buf)
		return nil, fmt.Errorf("failed to copy: %w", err)
	}

	var content io.ReadCloser
	if DataSize(n) > memLimit {
		if pp.file == nil {
			f, err := os.CreateTemp("", "formstream-")
			if err != nil {
				bufPool.Put(buf)
				return nil, fmt.Errorf("failed to create temp file: %w", err)
			}
			pp.file = f
		}

		bufSize, err := io.Copy(pp.file, buf)
		if err != nil {
			bufPool.Put(buf)
			return nil, fmt.Errorf("failed to write: %w", err)
		}

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			bufPool.Put(buf)
			return nil, fmt.Errorf("failed to copy: %w", err)
		}

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size

		bufPool.Put(buf)
	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()

		var closed bool
		rc := customReadCloser{
			Reader: buf,
			closeFunc: func() error {
				if closed {
					return nil
				}
				closed = true
				bufPool.Put(buf)
				pp.config.maxMemSize += DataSize(bufSize)
				pp.config.maxMemFileSize += DataSize(bufSize)
				return nil
			},
		}
		pp.pending = append(pp.pending, rc)
		content = rc
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	// Release every in-memory content that was buffered for a hook which
	// never ran (canceled or otherwise aborted parse).
	for _, content := range pp.pending {
		_ = content.Close()
	}
	pp.pending = nil

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
