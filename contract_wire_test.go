package formstream_test

import (
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"testing"

	"github.com/mazrean/formstream/internal/formtest"
)

// assertWireFraming validates the generated multipart body as a transport
// document, independent of the decoded field table:
//
//   - the exact boundary declared by the model is used on every delimiter,
//   - headers appear in public RFC 7578 order (Content-Disposition then
//     Content-Type), with CRLF framing throughout,
//   - the body ends with the closing delimiter exactly once and carries no
//     trailing bytes (explicit close state on the wire),
//   - payloads (including embedded boundary-looking bytes) are carried
//     verbatim and the part count matches.
func assertWireFraming(t testing.TB, wire []byte, parts []formtest.Part, spooled bool) {
	t.Helper()

	delim := []byte("--" + formtest.Boundary)
	closing := append(append([]byte{}, delim...), []byte("--")...)

	if !bytes.HasPrefix(wire, delim) {
		t.Fatalf("wire does not start with boundary delimiter %q; prefix=%q", delim, firstLine(wire))
	}
	if !bytes.HasSuffix(wire, []byte("\r\n--"+formtest.Boundary+"--\r\n")) {
		t.Fatalf("wire does not end with exactly one closing delimiter; suffix=%q", tail(wire, 64))
	}
	if n := bytes.Count(wire, closing); n != 1 {
		t.Fatalf("closing delimiter occurs %d times, want exactly 1", n)
	}

	all := parts
	if spooled {
		all = append(append([]formtest.Part(nil), parts...), formtest.Part{
			Name:    "marker",
			Content: []byte("done"),
		})
	}

	mr := multipart.NewReader(bytes.NewReader(wire), formtest.Boundary)
	idx := 0
	for {
		p, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("wire failed multipart re-parse at part %d: %v", idx, err)
		}
		if idx >= len(all) {
			t.Fatalf("wire exposes more parts than model: extra part name=%q", p.FormName())
		}
		want := all[idx]

		if p.Header.Get("Content-Disposition") == "" {
			t.Fatalf("part %d (%s): missing Content-Disposition", idx, want.Name)
		}
		block := headerBlock(wire, idx)
		if block == nil {
			t.Fatalf("part %d (%s): raw header block not located", idx, want.Name)
		}
		iDisp := bytes.Index(block, []byte("Content-Disposition:"))
		iType := bytes.Index(block, []byte("Content-Type:"))
		if iType >= 0 && (iDisp < 0 || iDisp > iType) {
			t.Fatalf("part %d (%s): header order violated: Content-Type before Content-Disposition in block %q",
				idx, want.Name, block)
		}
		if idx < len(all)-1 && !delimCRLF(wire, idx) {
			t.Fatalf("part %d (%s): delimiter not terminated by CRLF", idx, want.Name)
		}

		if p.FormName() != want.Name {
			t.Fatalf("part %d: name want %q, wire has %q", idx, want.Name, p.FormName())
		}
		if want.IsFile() {
			if p.FileName() != want.FileName {
				t.Fatalf("part %d: filename want %q, wire has %q", idx, want.FileName, p.FileName())
			}
			if want.ContentType != "" && p.Header.Get("Content-Type") != want.ContentType {
				t.Fatalf("part %d: content-type want %q, wire has %q",
					idx, want.ContentType, p.Header.Get("Content-Type"))
			}
		}

		payload, err := io.ReadAll(p)
		if err != nil {
			t.Fatalf("part %d (%s): reading payload from wire model: %v", idx, want.Name, err)
		}
		if !bytes.Equal(payload, want.Content) {
			t.Fatalf("part %d (%s): wire payload differs from model at %s (want %d bytes, got %d)",
				idx, want.Name, firstDiff(want.Content, payload), len(want.Content), len(payload))
		}
		idx++
	}
	if idx != len(all) {
		t.Fatalf("part count mismatch: model has %d parts, wire exposed %d", len(all), idx)
	}

	framingDelims := bytes.Count(wire, []byte("\r\n--"+formtest.Boundary))
	if framingDelims != len(all) {
		t.Fatalf("framed delimiter count mismatch: want %d, got %d", len(all), framingDelims)
	}
}

// headerBlock extracts the raw header bytes of the idx-th part to verify
// on-wire header ordering without reconstructing headers from parsed data.
func headerBlock(wire []byte, idx int) []byte {
	pos := 0
	for i := 0; i <= idx; i++ {
		lineEnd := bytes.Index(wire[pos:], []byte("\r\n"))
		if lineEnd < 0 {
			return nil
		}
		hdrStart := pos + lineEnd + 2
		j := bytes.Index(wire[hdrStart:], []byte("\r\n\r\n"))
		if j < 0 {
			return nil
		}
		block := wire[hdrStart : hdrStart+j]
		if i == idx {
			return block
		}
		pos = hdrStart + j + 4
		k := bytes.Index(wire[pos:], []byte("\r\n--"+formtest.Boundary))
		if k < 0 {
			return nil
		}
		pos += k + 2
	}
	return nil
}

// delimCRLF reports that the idx-th boundary delimiter is immediately
// followed by CRLF (canonical wire framing).
func delimCRLF(wire []byte, idx int) bool {
	marker := []byte("--" + formtest.Boundary)
	pos := 0
	for i := 0; i < idx; i++ {
		j := bytes.Index(wire[pos:], append([]byte("\r\n"), marker...))
		if j < 0 {
			return false
		}
		pos += j + 2
	}
	end := pos + len(marker)
	return end+2 <= len(wire) && wire[end] == '\r' && wire[end+1] == '\n'
}

func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return "..." + string(b[len(b)-n:])
}
