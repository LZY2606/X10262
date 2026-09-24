package formstream_test

// Wire-framing level checks: these cases cannot pass by merely returning
// the right parsed fields. They pin down boundary delimiters, header
// order, empty/Unicode file transport, and the closed state of the parse
// (every byte of the multipart document must be consumed as parts).
import (
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"strings"
	"testing"

	"github.com/mazrean/formstream"
)

func newMultipartReader(model formModel) *multipart.Reader {
	return multipart.NewReader(bytes.NewReader(model.wire), model.boundary)
}

// TestFormStreamTransportContract_WireFramingExact pins the exact wire:
// boundary prefix/terminator, CRLF usage and lexicographic header order
// produced by the model (Go's multipart.Writer emits sorted header keys),
// which is the public contract the parser must accept unchanged.
func TestFormStreamTransportContract_WireFramingExact(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	wire := string(model.wire)

	lines := strings.Split(wire, "\r\n")
	if lines[0] != "--"+contractBoundary {
		t.Fatalf("first violation: opening delimiter:\n want %q\n got  %q", "--"+contractBoundary, lines[0])
	}
	if lines[len(lines)-1] != "" || lines[len(lines)-2] != "--"+contractBoundary+"--" {
		t.Fatalf("first violation: closing delimiter:\n want %q as final CRLF-terminated line\n got tail %q",
			"--"+contractBoundary+"--", strings.Join(lines[len(lines)-3:], "|"))
	}
	if bytes.Contains(model.wire, []byte("\n--"+contractBoundary+"--\n")) {
		t.Fatal("first violation: wire uses bare LF framing; CRLF is required")
	}

	// Every file part with multiple headers must carry them in canonical
	// (lexicographic) wire order: Content-Disposition before Content-Type.
	var partStarts int
	for i := 0; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "--"+contractBoundary) && !strings.HasSuffix(lines[i], "--") {
			partStarts++
			if i+1 >= len(lines) {
				t.Fatalf("first violation: delimiter at line %d has no headers", i)
			}
			if !strings.HasPrefix(lines[i+1], "Content-Disposition:") {
				t.Errorf("first violation: part #%d header order:\n want first header Content-Disposition\n got  %q",
					partStarts, lines[i+1])
			}
		}
	}
	if partStarts != len(model.parts) {
		t.Errorf("first violation: framed part count want %d got %d", len(model.parts), partStarts)
	}

	// Unicode filename must survive on the wire as UTF-8 inside the quoted
	// disposition value (no percent-escaping, no loss).
	if !strings.Contains(wire, `filename="テスト-漢字.txt"`) {
		t.Errorf("first violation: unicode filename absent from wire: %q",
			firstLineContaining(wire, "fileUnicode"))
	}

	// Parser-side observable counterpart.
	got := collectResult(t, formstream.NewParser(model.boundary), model)
	exp := expectedResult(t, model)
	assertResultEqual(t, "exact wire", exp, got)
}

func firstLineContaining(wire, marker string) string {
	for _, l := range strings.Split(wire, "\r\n") {
		if strings.Contains(l, marker) {
			return l
		}
	}
	return ""
}

// TestFormStreamTransportContract_BoundaryAcrossChunks feeds the exact
// wire cut so that the leading "--", the closing "--" and every interior
// delimiter straddle two reads. Parsed values and framing both asserted.
func TestFormStreamTransportContract_BoundaryAcrossChunks(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	exp := expectedResult(t, model)

	delimiter := []byte("--" + contractBoundary)
	var cutOffsets []int
	for i := 0; i+len(delimiter) <= len(model.wire); i++ {
		if bytes.Equal(model.wire[i:i+len(delimiter)], delimiter) {
			cutOffsets = append(cutOffsets, i+len(delimiter)/2)
		}
	}
	if len(cutOffsets) != len(model.parts)+1 {
		t.Fatalf("first violation: expected %d delimiter cuts, found %d", len(model.parts)+1, len(cutOffsets))
	}

	for idx, at := range cutOffsets {
		at := at
		t.Run("delimiterCut_"+itoa(idx)+"_at_"+itoa(at), func(t *testing.T) {
			t.Parallel()
			p := formstream.NewParser(model.boundary)
			got := newParsedForm()
			hookRecorder(t, p, model, got, nil)
			err := p.Parse(&chunkReader{data: model.wire, sizes: []int{at, 0}})
			if err != nil {
				t.Fatalf("first violation: parse with delimiter straddling chunks at %d: %v", at, err)
			}
			fillValues(t, p, got)
			assertResultEqual(t, "boundary-split@"+itoa(at), exp, got)
			assertWireCanonical(t, "boundary-split@"+itoa(at), model, got.hooks)
		})
	}
}

// TestFormStreamTransportContract_EmptyFileAndUnicode exercise the two
// edge payloads explicitly, including a 1-byte chunking so the empty
// part's zero-length body and the multibyte name travel byte by byte.
func TestFormStreamTransportContract_EmptyFileAndUnicode(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)

	t.Run("empty file yields zero length stream once", func(t *testing.T) {
		t.Parallel()
		p := formstream.NewParser(model.boundary)
		var calls int
		var n int64
		err := p.Register("fileEmpty", func(r io.Reader, _ formstream.Header) error {
			calls++
			var err error
			n, err = io.Copy(io.Discard, r)
			return err
		})
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if err := p.Parse(&chunkReader{data: model.wire, sizes: ones(len(model.wire))}); err != nil {
			t.Fatalf("first violation: parse: %v", err)
		}
		if calls != 1 {
			t.Errorf("first violation: empty file hook invocations want 1 got %d", calls)
		}
		if n != 0 {
			t.Errorf("first violation: empty file bytes delivered want 0 got %d", n)
		}
	})

	t.Run("unicode filename round trips byte by byte", func(t *testing.T) {
		t.Parallel()
		p := formstream.NewParser(model.boundary)
		got := newParsedForm()
		hookRecorder(t, p, model, got, nil)
		if err := p.Parse(&chunkReader{data: model.wire, sizes: ones(len(model.wire))}); err != nil {
			t.Fatalf("first violation: parse: %v", err)
		}
		if got.names["fileUnicode"] != "テスト-漢字.txt" {
			t.Errorf("first violation: unicode filename:\n want %q\n got  %q", "テスト-漢字.txt", got.names["fileUnicode"])
		}
		if got.files["fileUnicode"] == "" {
			t.Error("first violation: unicode file body not delivered")
		}
	})
}

// TestFormStreamTransportContract_CRLFHeaderOrderManuallyBuilt confirms
// the parser accepts a hand-authored wire (not generated by the model
// writer) whose header block pins exact order and exact closing state.
func TestFormStreamTransportContract_CRLFHeaderOrderManuallyBuilt(t *testing.T) {
	t.Parallel()

	body := "--" + contractBoundary + "\r\n" +
		"Content-Disposition: form-data; name=\"f\"; filename=\"a.txt\"\r\n" +
		"Content-Type: text/plain\r\n" +
		"X-Custom: 1\r\n" +
		"\r\n" +
		"abc" +
		"\r\n--" + contractBoundary + "--\r\n"

	p := formstream.NewParser(contractBoundary)
	var seen []string
	var gotContent string
	err := p.Register("f", func(r io.Reader, h formstream.Header) error {
		seen = append(seen,
			h.Get("Content-Disposition"),
			h.Get("Content-Type"),
			h.Get("X-Custom"))
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		gotContent = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Parse(strings.NewReader(body)); err != nil {
		t.Fatalf("first violation: hand built wire rejected: %v", err)
	}
	if gotContent != "abc" {
		t.Errorf("first violation: file body want abc got %q", gotContent)
	}
	if seen[1] != "text/plain" || seen[2] != "1" {
		t.Errorf("first violation: header observability order/values: %v", seen)
	}

	// Malformed framing: header line without a colon must surface as a
	// parse-layer cause, repeatably, never as success.
	bad := "--" + contractBoundary + "\r\n" +
		"Content-Disposition form-data; name=\"x\"\r\n\r\nv\r\n" +
		"--" + contractBoundary + "--\r\n"
	if err := formstream.NewParser(contractBoundary).Parse(strings.NewReader(bad)); err == nil {
		t.Error("first violation: malformed header framing was accepted")
	} else if err := formstream.NewParser(contractBoundary).Parse(strings.NewReader(bad)); err == nil {
		t.Error("first violation: malformed framing rejection is not repeatable")
	}
}

// TestFormStreamTransportContract_TruncatedBodyCause distinguishes a
// truncated document (unexpected EOF cause) from a clean one.
func TestFormStreamTransportContract_TruncatedBodyCause(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	truncated := model.wire[:bytes.LastIndex(model.wire, []byte("--"+contractBoundary+"--"))]

	err := formstream.NewParser(model.boundary).Parse(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("first violation: truncated multipart body accepted as complete")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("first violation: truncated cause lost:\n want io.ErrUnexpectedEOF chain\n got  %v", err)
	}
}
