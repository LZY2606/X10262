package formstream_test

// Regression materials for the transport contract:
//
//	"formstream: arbitrary chunking, cancellation and writer errors"
//
// Every case enters the library through the public API (NewParser /
// Register / Parse and the exported query methods). No internal parser
// state is constructed by these tests. The same fixed-seed input model is
// re-run under different chunking, ordering and scheduling conditions and
// is checked not only for parsed values but also for wire framing
// (boundary delimiters, header order, termination), cleanup (temp files
// and goroutines), single error reporting and retry stability after a
// recoverable failure.
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/mazrean/formstream"
)

const contractBoundary = "fscontractB0123456789"

type partSpec struct {
	name     string
	filename string
	content  []byte
	headers  map[string]string
}

type formModel struct {
	boundary string
	parts    []partSpec
	wire     []byte
}

// buildFixedSeedModel builds a multipart body from fixed-seed generated
// fields and files: text fields, a small file, an empty file and a file
// whose name is non-ASCII. Content deliberately contains CRLF-free,
// boundary-free bytes so it cannot masquerade as framing.
func buildFixedSeedModel(t *testing.T, boundary string, opts ...func(*[]partSpec)) formModel {
	t.Helper()

	rng := rand.New(rand.NewSource(20260925))

	randBytes := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			// avoid '\r' and '-' so content cannot fake a boundary
			b[i] = byte('a' + rng.Intn(25))
		}
		return b
	}

	parts := []partSpec{
		{name: "fieldA", content: []byte("value-A")},
		{name: "dup", content: randBytes(7)},
		{
			name:     "fileSmall",
			filename: "small.txt",
			content:  randBytes(37),
			headers:  map[string]string{"Content-Type": "text/plain"},
		},
		{
			name:     "fileUnicode",
			filename: "テスト-漢字.txt",
			content:  append(append([]byte{}, randBytes(19)...), 'z'),
			headers:  map[string]string{"Content-Type": "text/plain"},
		},
		{name: "fileEmpty", filename: "empty.bin", content: nil, headers: map[string]string{"Content-Type": "application/octet-stream"}},
		{name: "dup", content: []byte("second-dup")},
		{name: "fieldB", content: []byte("v\ndata-2")},
	}
	for _, opt := range opts {
		opt(&parts)
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("model: set boundary: %v", err)
	}
	for _, ps := range parts {
		h := make(textproto.MIMEHeader)
		var keys []string
		h.Set("Content-Disposition", contentDisposition(ps))
		keys = append(keys, "Content-Disposition")
		for k, v := range ps.headers {
			h.Set(k, v)
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pw, err := mw.CreatePart(h)
		if err != nil {
			t.Fatalf("model: create part %q: %v", ps.name, err)
		}
		if len(ps.content) > 0 {
			if _, err := pw.Write(ps.content); err != nil {
				t.Fatalf("model: write part %q: %v", ps.name, err)
			}
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("model: close: %v", err)
	}

	return formModel{boundary: boundary, parts: parts, wire: buf.Bytes()}
}

func contentDisposition(ps partSpec) string {
	if ps.filename != "" {
		return multipart.FileContentDisposition(ps.name, ps.filename)
	}
	return fmt.Sprintf(`form-data; name=%q`, ps.name)
}

// writeDispositionPart is a small model helper for ad-hoc fault fixtures:
// it writes one file part with a disposition and content type using the
// standard multipart writer (the same production-grade framing the
// parser consumes through its public Parse entry).
func writeDispositionPart(t *testing.T, mw *multipart.Writer, name, filename, contentType string, content []byte) {
	t.Helper()
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", multipart.FileContentDisposition(name, filename))
	h.Set("Content-Type", contentType)
	pw, err := mw.CreatePart(h)
	if err != nil {
		t.Fatalf("model: create part %q: %v", name, err)
	}
	if len(content) > 0 {
		if _, err := pw.Write(content); err != nil {
			t.Fatalf("model: write part %q: %v", name, err)
		}
	}
}

// parsedForm is the observable result of one parse run, independent of
// how the bytes were delivered.
type parsedForm struct {
	values map[string][]string
	files  map[string]string // field name -> observed file content
	names  map[string]string // field name -> first observed filename
	ctypes map[string]string // field name -> first observed content type
	hooks  []string          // hook invocation order
}

// hookRecorder registers one streaming hook per file-bearing part and
// consumes every hook exactly once. All three parsers (fast path and the
// two spool paths) share this single production callback.
func hookRecorder(t *testing.T, p *formstream.Parser, model formModel, got *parsedForm, hookErr func(ps partSpec) error) {
	t.Helper()
	for _, ps := range model.parts {
		ps := ps
		if ps.filename == "" {
			continue
		}
		err := p.Register(ps.name, func(r io.Reader, h formstream.Header) error {
			b, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("hook %q read: %w", ps.name, err)
			}
			got.hooks = append(got.hooks, ps.name)
			got.files[ps.name] += string(b)
			got.names[ps.name] = h.FileName()
			got.ctypes[ps.name] = h.ContentType()
			if hookErr != nil {
				return hookErr(ps)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("register %q: %v", ps.name, err)
		}
	}
}

func newParsedForm() *parsedForm {
	return &parsedForm{
		values: make(map[string][]string),
		files:  make(map[string]string),
		names:  make(map[string]string),
		ctypes: make(map[string]string),
	}
}

func collectResult(t *testing.T, p *formstream.Parser, model formModel) *parsedForm {
	t.Helper()
	got := newParsedForm()
	hookRecorder(t, p, model, got, nil)
	if err := p.Parse(bytes.NewReader(model.wire)); err != nil {
		t.Fatalf("parse: %v", err)
	}
	vm := p.ValueMap()
	for key, vs := range vm {
		for _, v := range vs {
			s, _ := v.Unwrap()
			got.values[key] = append(got.values[key], s)
		}
	}
	return got
}

// expectedResult parses the same wire with the standard library to build
// the observable oracle for fields, then layers in the model for files.
func expectedResult(t *testing.T, model formModel) *parsedForm {
	t.Helper()
	exp := newParsedForm()
	mr := multipart.NewReader(bytes.NewReader(model.wire), model.boundary)
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("oracle: next part: %v", err)
		}
		b, err := io.ReadAll(part)
		if err != nil {
			t.Fatalf("oracle: read part: %v", err)
		}
		if part.FileName() != "" {
			exp.files[part.FormName()] += string(b)
			exp.names[part.FormName()] = part.FileName()
			exp.ctypes[part.FormName()] = part.Header.Get("Content-Type")
		} else {
			exp.values[part.FormName()] = append(exp.values[part.FormName()], string(b))
		}
	}
	return exp
}

// assertResultEqual compares every observable field and records the first
// violation with enough context to diagnose it (not a pass/fail boolean).
func assertResultEqual(t *testing.T, tag string, exp, got *parsedForm) {
	t.Helper()
	keys := func(m map[string][]string) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	if expKeys, gotKeys := keys(exp.values), keys(got.values); !equalStrings(expKeys, gotKeys) {
		t.Errorf("%s: value key set mismatch:\n first violation at field keys\n want %v\n got  %v", tag, expKeys, gotKeys)
		return
	}
	for _, key := range keys(exp.values) {
		if !equalStrings(exp.values[key], got.values[key]) {
			t.Errorf("%s: field %q value order/content mismatch:\n want %q\n got  %q", tag, key, exp.values[key], got.values[key])
		}
	}
	if len(exp.files) != len(got.files) {
		t.Errorf("%s: file key count mismatch: want %d got %d", tag, len(exp.files), len(got.files))
	}
	for name, want := range exp.files {
		if got.files[name] != want {
			t.Errorf("%s: file %q content mismatch:\n want %q (%d bytes)\n got  %q (%d bytes)", tag, name, want, len(want), got.files[name], len(got.files[name]))
		}
		if got.names[name] != exp.names[name] {
			t.Errorf("%s: file %q unicode/filename transport mismatch:\n want %q\n got  %q", tag, name, exp.names[name], got.names[name])
		}
		if got.ctypes[name] != exp.ctypes[name] {
			t.Errorf("%s: file %q content-type mismatch:\n want %q\n got  %q", tag, name, exp.ctypes[name], got.ctypes[name])
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// chunkReader delivers wire bytes in a fixed sequence of chunk sizes.
// size <= 0 means "read with a large buffer", i.e. whatever the parser
// asks for; size == 1 drives the one-byte-at-a-time extreme.
type chunkReader struct {
	data  []byte
	pos   int
	sizes []int
	idx   int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := len(p)
	if c.idx < len(c.sizes) {
		if s := c.sizes[c.idx]; s > 0 && s < n {
			n = s
		}
		c.idx++
	}
	if remain := len(c.data) - c.pos; n > remain {
		n = remain
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

// countFormstreamTempFiles reports formstream-* temp files visible on disk.
func countFormstreamTempFiles(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if ok, _ := filepath.Match("formstream-*", e.Name()); ok {
			n++
		}
	}
	return n
}
