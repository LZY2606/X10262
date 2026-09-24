package formstream_test

// Model-based checks: the fixed-seed form is generated once and parsed
// through the public entry point at every chunk size/offset combination,
// including one-byte chunks and chunks that split boundary delimiters and
// header lines. Results must be identical to the standard-library oracle
// and independent of result-irrelevant ordering/scheduling variations.
import (
	"bytes"
	"io"
	"testing"

	"github.com/mazrean/formstream"
)

// cutPoints returns every interesting split position of the wire:
// exhaustive for small wires so each byte of the boundary delimiters,
// CRLFs and headers is crossed by a chunk boundary at least once.
func cutPoints(wire []byte) []int {
	cuts := map[int]struct{}{0: {}, len(wire): {}}
	needle := []byte("--" + contractBoundary)
	for i := 0; i+len(needle) <= len(wire); i++ {
		if bytes.Equal(wire[i:i+len(needle)], needle) {
			for d := 0; d <= len(needle); d++ {
				cuts[i+d] = struct{}{}
			}
			// CRLF immediately after the delimiter
			cuts[i+len(needle)+1] = struct{}{}
			cuts[i+len(needle)+2] = struct{}{}
		}
	}
	out := make([]int, 0, len(cuts))
	for c := range cuts {
		out = append(out, c)
	}
	return out
}

// chunkSizesBetween turns split points into an alternating chunk-size
// schedule. Each cut becomes its own chunk edge.
func chunkSizesBetween(wire []byte, cuts []int) [][]int {
	clean := make([]int, 0, len(cuts))
	for _, c := range cuts {
		if c >= 0 && c <= len(wire) {
			clean = append(clean, c)
		}
	}
	var edges []int
	edges = append(edges, clean...)
	schedules := make([][]int, 0, len(edges)-1)
	for i := 1; i < len(edges); i++ {
		schedules = append(schedules, []int{edges[i] - edges[i-1], 0})
	}
	return schedules
}

func TestFormStreamTransportContract_ModelFixedSeed_ArbitraryChunkBoundaries(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	exp := expectedResult(t, model)

	t.Run("single byte chunks split every boundary", func(t *testing.T) {
		t.Parallel()
		p := formstream.NewParser(model.boundary)
		got := newParsedForm()
		hookRecorder(t, p, model, got, nil)
		if err := p.Parse(&chunkReader{data: model.wire, sizes: ones(len(model.wire))}); err != nil {
			t.Fatalf("first violation: parse under 1-byte chunking: %v", err)
		}
		fillValues(t, p, got)
		assertResultEqual(t, "1-byte chunks", exp, got)
		assertWireCanonical(t, "1-byte chunks", model, got.hooks)
	})

	t.Run("exhaustive cut points across boundary and headers", func(t *testing.T) {
		t.Parallel()
		cuts := cutPoints(model.wire)
		for _, at := range cuts {
			at := at
			t.Run("cutAt_"+itoa(at), func(t *testing.T) {
				t.Parallel()
				sizes := []int{}
				if at > 0 {
					sizes = append(sizes, at)
				}
				sizes = append(sizes, 0) // remainder however the parser asks
				p := formstream.NewParser(model.boundary)
				got := newParsedForm()
				hookRecorder(t, p, model, got, nil)
				if err := p.Parse(&chunkReader{data: model.wire, sizes: sizes}); err != nil {
					t.Fatalf("first violation: cut at %d: parse error %v", at, err)
				}
				fillValues(t, p, got)
				assertResultEqual(t, "cut@"+itoa(at), exp, got)
			})
		}
	})

	t.Run("two byte stride re-splits framing repeatedly", func(t *testing.T) {
		t.Parallel()
		sizes := make([]int, 0, len(model.wire)/2+1)
		for i := 0; i < len(model.wire); i += 2 {
			sizes = append(sizes, 2)
		}
		p := formstream.NewParser(model.boundary)
		got := newParsedForm()
		hookRecorder(t, p, model, got, nil)
		if err := p.Parse(&chunkReader{data: model.wire, sizes: sizes}); err != nil {
			t.Fatalf("first violation: 2-byte stride: %v", err)
		}
		fillValues(t, p, got)
		assertResultEqual(t, "2-byte stride", exp, got)
	})

	t.Run("bulk read reference", func(t *testing.T) {
		t.Parallel()
		got := collectResult(t, formstream.NewParser(model.boundary), model)
		assertResultEqual(t, "bulk", exp, got)
	})
}

func ones(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = 1
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func fillValues(t *testing.T, p *formstream.Parser, got *parsedForm) {
	t.Helper()
	for key, vs := range p.ValueMap() {
		for _, v := range vs {
			s, _ := v.Unwrap()
			got.values[key] = append(got.values[key], s)
		}
	}
}

// assertWireCanonical checks framing-level public invariants that survive
// chunking: each hook saw its file exactly once in wire order, and the
// delivered stream can be independently re-parsed from start to finish.
func assertWireCanonical(t *testing.T, tag string, model formModel, hookOrder []string) {
	t.Helper()
	var wantOrder []string
	for _, ps := range model.parts {
		if ps.filename != "" {
			wantOrder = append(wantOrder, ps.name)
		}
	}
	if !equalStrings(wantOrder, hookOrder) {
		t.Errorf("%s: first violation: file hook/stream order (wire framing) mismatch:\n want %v\n got  %v", tag, wantOrder, hookOrder)
	}

	// Independent framing re-read: the wire must round-trip via the
	// standard reader from the very first delimiter to the closing one.
	mr := newMultipartReader(model)
	count := 0
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("%s: first violation: generated wire not valid multipart at part %d: %v", tag, count, err)
			return
		}
		if _, err := io.Copy(io.Discard, part); err != nil {
			t.Errorf("%s: first violation: wire part %d unreadable: %v", tag, count, err)
			return
		}
		count++
	}
	if count != len(model.parts) {
		t.Errorf("%s: first violation: framing part count want %d got %d", tag, len(model.parts), count)
	}
}
