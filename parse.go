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
// It is equivalent to ParseWithContext with context.Background.
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseWithContext(context.Background(), r)
}

// ParseWithContext parses the multipart form from r.
//
// The source reader is consumed on an internal pump goroutine through an
// io.Pipe so that cancellation does not depend on the parser reaching a part
// boundary: canceling ctx closes the read side of the pipe, which unwinds the
// parser immediately. A Read on r that is blocked when ctx is canceled is
// expected to return ctx.Err (as request bodies do); the parser result then
// reports ctx.Err exactly once and the pump goroutine and the pipe are
// released before ParseWithContext returns.
func (p *Parser) ParseWithContext(ctx context.Context, r io.Reader) (err error) {
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

	err = p.consume(ctx, r, hsc.IConditionJudger)

	return
}

// consume runs the source reader on a pump goroutine and parses whatever the
// pump forwards. It is the single transport path: ParseWithContext and
// therefore every framework wrapper go through it.
func (p *Parser) consume(
	ctx context.Context,
	r io.Reader,
	hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam],
) error {
	pr, pw := io.Pipe()

	// Exactly one terminal cause is ever reported on the pipe: the source
	// error, io.EOF on a complete stream, or ctx.Err on cancellation.
	var closeOnce sync.Once
	closeRead := func(cause error) {
		closeOnce.Do(func() {
			pr.CloseWithError(cause)
		})
	}

	var pumpDone sync.WaitGroup
	pumpDone.Add(1)
	go func() {
		defer pumpDone.Done()
		// Closing the writer after the pump stops releases the pipe even when
		// the parser has already been unblocked by closing the reader.
		defer pw.Close()

		buf := make([]byte, 32*1024)
		for {
			n, readErr := r.Read(buf)
			if n > 0 {
				pending := buf[:n]
				for len(pending) > 0 {
					m, writeErr := pw.Write(pending)
					pending = pending[m:]
					if writeErr != nil {
						// The read side was closed (cancellation). The
						// terminal cause was already recorded there.
						return
					}
				}
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					closeRead(nil)
				} else {
					closeRead(readErr)
				}
				return
			}
		}
	}()
	// AfterFunc fires in its own goroutine, so cancellation is delivered even
	// while the parser is blocked inside a hook or a pipe read.
	stopAfterFunc := context.AfterFunc(ctx, func() {
		closeRead(ctx.Err())
	})
	defer stopAfterFunc()

	parseErr := p.parse(pr, hsc)

	// Make sure the pump has observed the closed pipe before returning so no
	// transport goroutine outlives the call.
	_ = pr.Close()
	pumpDone.Wait()

	return parseErr
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
	config    *parserConfig
	offset    int64
	file      tempFile
	activeMem []*customReadCloser
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// tempFile is the on-disk spill target. It is an internal seam: production
// uses newTempFile below (os.CreateTemp, the only algorithm in this path),
// while tests substitute a fault-injecting writer that runs through the exact
// same io.Copy logic.
type tempFile interface {
	io.Writer
	io.ReaderAt
	io.Closer
	Name() string
}

var newTempFile = func() (tempFile, error) {
	return os.CreateTemp("", "formstream-")
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
			f, err := newTempFile()
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

		idx := len(pp.activeMem)
		memContent := customReadCloser{
			Reader: buf,
			closeFunc: func() error {
				bufPool.Put(buf)
				pp.config.maxMemSize += DataSize(bufSize)
				pp.config.maxMemFileSize += DataSize(bufSize)
				pp.activeMem[idx] = nil
				return nil
			},
		}
		pp.activeMem = append(pp.activeMem, &memContent)
		content = memContent
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	// Release memory contents of hooks that were buffered waiting for their
	// required parts but never executed because parsing failed or was
	// canceled. Closing through closeFunc restores pool entries and budget
	// counters exactly once (already-closed entries are nil).
	var errs []error
	for _, c := range pp.activeMem {
		if c != nil {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	pp.activeMem = nil

	if pp.file == nil {
		return errors.Join(errs...)
	}

	filepath := pp.file.Name()

	// Close the file handle first
	closeErr := pp.file.Close()

	// Remove the temporary file from disk
	removeErr := os.Remove(filepath)

	// Return combined errors if any
	if closeErr != nil || removeErr != nil {
		errs = append(errs, closeErr, removeErr)
	}

	return errors.Join(errs...)
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
