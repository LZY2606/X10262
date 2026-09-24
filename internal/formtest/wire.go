// Package formtest provides fixtures and fault injectors for testing the
// multipart streaming transport contract of formstream.
//
// Every fixture builds a real multipart/form-data wire payload with the
// standard mime/multipart writer and re-enters the library exclusively through
// its public parsing entry points. Nothing in this package reproduces parser
// internals: it is test-only transport machinery (chunk schedules, source
// failures, short writers, cancellation pipes and leak probes).
package formtest

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"mime/multipart"
	"net/textproto"
	"strconv"
)

// Boundary is the fixed boundary used by seeded fixtures.
const Boundary = "formstreamcontract"

// Sentinel errors reported by fault injectors. They carry unique messages so
// that the number of times a cause appears on a wrapped error chain can be
// counted exactly.
var (
	// ErrSourceFailed is injected by a source reader in the middle of a body.
	ErrSourceFailed = fmt.Errorf("formtest: source reader failed at injected point")
	// ErrWriterFailed is injected by a target writer after it accepted data.
	ErrWriterFailed = fmt.Errorf("formtest: target writer failed at injected point")
)

// Part describes one part of the wire model.
type Part struct {
	// Name is the form field name.
	Name string
	// FileName is empty for regular fields. When non-empty the part is a file.
	FileName string
	// ContentType is emitted only for files.
	ContentType string
	// Content is the exact decoded payload the parser must observe.
	Content []byte
}

// IsFile reports whether the part is modeled as an uploaded file.
func (p Part) IsFile() bool {
	return p.FileName != ""
}

// SeededParts deterministically generates the fixed set of fields and files
// used by the transport-contract matrix. The seed is fixed on purpose: every
// case below runs against this exact body so that failures stay reproducible.
//
// The set deliberately includes an empty file, a Unicode filename, a file
// whose payload contains the boundary marker (boundary-safe framing stress)
// and duplicate field names.
func SeededParts(seed uint64) []Part {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	bin := make([]byte, 2048)
	for i := range bin {
		bin[i] = byte(rng.Uint32())
	}
	// Embed the boundary marker and CRLF inside payload: a naive framing
	// implementation that searches raw bytes (instead of the delimiter
	// expected at a part boundary) is broken by this.
	copy(bin[500:], []byte("--"+Boundary))
	copy(bin[1200:], []byte("--"+Boundary+"-partial"))

	text := make([]byte, 1500)
	for i := range text {
		text[i] = byte('a' + rng.IntN(26))
	}

	return []Part{
		{Name: "field0", Content: []byte("value-0")},
		{
			Name:        "empty",
			FileName:    "empty.txt",
			ContentType: "text/plain",
			Content:     []byte{},
		},
		{
			Name:        "notes",
			FileName:    "notes.txt",
			ContentType: "text/plain",
			Content:     text,
		},
		{Name: "field1", Content: []byte("日本語フィールド値-🚴")},
		{
			Name:        "unicode",
			FileName:    "テスト-🚴.txt",
			ContentType: "text/plain; charset=utf-8",
			Content:     []byte("unicode file contents: café ☕\r\nline2"),
		},
		{
			Name:        "bin",
			FileName:    "binary.bin",
			ContentType: "application/octet-stream",
			Content:     bin,
		},
		{Name: "dup", Content: []byte("dup-one")},
		{Name: "dup", Content: []byte("dup-two")},
	}
}

// BuildWire serializes parts into a single multipart/form-data body with the
// fixed boundary and mime/multipart's canonical framing.
func BuildWire(parts []Part) ([]byte, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.SetBoundary(Boundary); err != nil {
		return nil, fmt.Errorf("formtest: set boundary: %w", err)
	}

	for _, p := range parts {
		if err := writePart(mw, p); err != nil {
			return nil, fmt.Errorf("formtest: write part %q: %w", p.Name, err)
		}
	}

	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("formtest: close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// writePart emits headers in the public RFC 7578 order: Content-Disposition
// first, then Content-Type. The Unicode filename is emitted verbatim (the
// same behavior as mime/multipart.Writer) to model real-world clients.
func writePart(mw *multipart.Writer, p Part) error {
	mh := make(textproto.MIMEHeader)
	if p.IsFile() {
		mh.Set("Content-Disposition",
			`form-data; name="`+escapeQuoted(p.Name)+`"; filename="`+escapeQuoted(p.FileName)+`"`)
		if p.ContentType != "" {
			mh.Set("Content-Type", p.ContentType)
		}
		pw, err := mw.CreatePart(mh)
		if err != nil {
			return err
		}
		if len(p.Content) > 0 {
			if _, err := pw.Write(p.Content); err != nil {
				return err
			}
		}
		return nil
	}

	mh.Set("Content-Disposition", `form-data; name="`+escapeQuoted(p.Name)+`"`)
	pw, err := mw.CreatePart(mh)
	if err != nil {
		return err
	}
	if _, err := pw.Write(p.Content); err != nil {
		return err
	}
	return nil
}

func escapeQuoted(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\r':
			b.WriteString("%0D")
		case '\n':
			b.WriteString("%0A")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// MinimalParts returns the smallest body that still exercises every framing
// construct: a field, a one-byte file, an empty file and a file whose
// content equals a boundary fragment.
func MinimalParts() []Part {
	return []Part{
		{Name: "f", Content: []byte("v")},
		{
			Name:        "g",
			FileName:    "g.txt",
			ContentType: "text/plain",
			Content:     []byte("x"),
		},
		{
			Name:        "h",
			FileName:    "empty.txt",
			ContentType: "text/plain",
			Content:     nil,
		},
		{
			Name:        "j",
			FileName:    "j.txt",
			ContentType: "text/plain",
			Content:     []byte("--" + Boundary),
		},
	}
}

// Schedule is a chunk-size schedule for feeding a body to a parser.
type Schedule []int

// OneByteChunks returns a one-byte-at-a-time schedule.
func OneByteChunks(n int) Schedule {
	s := make(Schedule, n)
	for i := range s {
		s[i] = 1
	}
	return s
}

// SeededSchedule returns a deterministic pseudo-random schedule of 1..maxN
// byte chunks. The last emitted chunk may be shorter.
func SeededSchedule(seed uint64, total, maxN int) Schedule {
	rng := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))
	var s Schedule
	remaining := total
	for remaining > 0 {
		n := 1 + rng.IntN(maxN)
		if n > remaining {
			n = remaining
		}
		s = append(s, n)
		remaining -= n
	}
	return s
}

// BoundarySplitSchedules returns two-chunk schedules that cut at every
// position from the last bytes of a part payload through the closing
// boundary. Each schedule covers a distinct "boundary crosses a chunk" point.
func BoundarySplitSchedules(wire []byte) []struct {
	Name     string
	Schedule Schedule
} {
	closing := []byte("\r\n--" + Boundary + "--\r\n")
	idx := bytes.LastIndex(wire, closing)
	if idx < 0 {
		return nil
	}
	start := idx - 1
	if start < 0 {
		start = 0
	}
	var out []struct {
		Name     string
		Schedule Schedule
	}
	for cut := start; cut < len(wire); cut++ {
		out = append(out, struct {
			Name     string
			Schedule Schedule
		}{
			Name:     "cut-at-" + strconv.Itoa(cut),
			Schedule: Schedule{cut, len(wire) - cut},
		})
	}
	return out
}
