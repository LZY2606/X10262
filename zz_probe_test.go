package formstream_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

func genBodyProbe(bdry string, parts [][2]string) []byte {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	_ = mw.SetBoundary(bdry)
	for _, p := range parts {
		_ = mw.WriteField(p[0], p[1])
	}
	mw.Close()
	return b.Bytes()
}

type flakyReader struct {
	data   []byte
	pos    int
	failAt int
	failed bool
	reads  int
}

var errFlaky = errors.New("flaky one-shot transport failure")

func (r *flakyReader) Read(p []byte) (int, error) {
	if !r.failed && r.pos < r.failAt && r.failAt <= r.pos+len(p) {
		n := r.failAt - r.pos
		copy(p, r.data[r.pos:r.failAt])
		r.pos = r.failAt
		r.failed = true
		r.reads++
		return n, errFlaky
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	r.reads++
	return n, nil
}

func TestProbeFlaky(t *testing.T) {
	body := genBodyProbe("B", [][2]string{{"a", strings.Repeat("x", 100)}, {"b", "y"}})
	fr := &flakyReader{data: body, failAt: 60}
	p := formstream.NewParser("B")
	err := p.Parse(fr)
	fmt.Printf("flaky: err=%v, reads=%d, isFlaky=%v\n", err, fr.reads, errors.Is(err, errFlaky))
}

func TestProbeShortWriter(t *testing.T) {
	body := genBodyProbe("B", [][2]string{{"file", "abcdefgh"}})
	p := formstream.NewParser("B")
	err := p.Register("file", func(r io.Reader, h formstream.Header) error {
		sw := &shortW{}
		_, e := io.Copy(sw, r)
		fmt.Printf("copy err: %v\n", e)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	perr := p.Parse(bytes.NewReader(body))
	fmt.Printf("parse err: %v\n", perr)
}

type shortW struct{}

func (shortW) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestProbeCancel(t *testing.T) {
	body := genBodyProbe("B", [][2]string{{"a", "v"}, {"f", strings.Repeat("z", 100000)}})
	pr, pw := io.Pipe()
	before := runtime.NumGoroutine()
	p := formstream.NewParser("B")
	_ = p.Register("f", func(r io.Reader, h formstream.Header) error {
		buf := make([]byte, 5)
		io.ReadFull(r, buf)
		return errors.New("hook canceled")
	})
	go func() {
		for i := 0; i < len(body); i += 64 {
			end := i + 64
			if end > len(body) {
				end = len(body)
			}
			_, _ = pw.Write(body[i:end])
			time.Sleep(time.Millisecond)
		}
		pw.Close()
	}()
	err := p.Parse(pr)
	fmt.Printf("cancel parse err: %v\n", err)
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	fmt.Printf("goroutines before=%d after=%d\n", before, after)
}

func TestProbeUnicode(t *testing.T) {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	_ = mw.SetBoundary("B")
	mh := make(textproto.MIMEHeader)
	mh.Set("Content-Disposition", `form-data; name="f"; filename="テスト-日本語-😀.txt"`)
	mh.Set("Content-Type", "text/plain")
	w, _ := mw.CreatePart(mh)
	w.Write([]byte("hello"))
	mw.Close()
	p := formstream.NewParser("B")
	var got string
	_ = p.Register("f", func(r io.Reader, h formstream.Header) error {
		got = h.FileName()
		d, _ := io.ReadAll(r)
		fmt.Printf("parsed filename=%q content=%q\n", got, d)
		return nil
	})
	err := p.Parse(bytes.NewReader(b.Bytes()))
	fmt.Printf("err=%v roundtrip=%v\n", err, got == "テスト-日本語-😀.txt")
}
