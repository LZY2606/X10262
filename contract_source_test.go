package formstream_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/formtest"
)

// TestFormStreamContract_SourceReaderFailsMidStream drives a permanent
// source failure at several offsets: mid-header, mid-payload and exactly on
// a boundary. The causal error must survive the wrap chain exactly once, no
// temp files may remain, and an identical retry with a healthy source must
// produce a stable full result.
func TestFormStreamContract_SourceReaderFailsMidStream(t *testing.T) {
	parts := formtest.SeededParts(2)
	wire := mustWire(t, parts)

	offsets := []struct {
		name string
		at   int64
	}{
		{"first-byte", 0},
		{"mid-first-header", 8},
		{"mid-first-payload", int64(indexOrDie(wire, "value-0") + 3)},
		{"on-boundary", int64(boundaryAt(wire, 2))},
		{"near-end", int64(len(wire) - 4)},
	}

	for _, o := range offsets {
		o := o
		t.Run(o.name, func(t *testing.T) {
			before := formtest.TempFiles(t)

			fr := formtest.NewFailingReader(wire, o.at, formtest.ErrSourceFailed)
			obs := runParse(t, fr, parts, parseOptions{})
			if obs.err == nil {
				t.Fatal("Parse succeeded with a failing source, expected an error")
			}
			if !errors.Is(obs.err, formtest.ErrSourceFailed) {
				t.Fatalf("source cause lost from error chain; chain:%s", formtest.DescribeError(obs.err))
			}
			if n := formtest.CountExactCause(obs.err, formtest.ErrSourceFailed); n != 1 {
				t.Fatalf("source cause reported %d times, want exactly 1; chain:%s", n, formtest.DescribeError(obs.err))
			}
			if fr.Attempts() > len(wire) {
				t.Fatalf("parser kept draining a permanently failed source: %d Read attempts", fr.Attempts())
			}
			formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))

			// Recoverable at the transport level: a fresh run with the
			// identical body succeeds and matches the model.
			retry := runParse(t, byteChunks(wire, formtest.OneByteChunks(len(wire))), parts, parseOptions{})
			assertSuccess(t, retry, parts, false)
		})
	}
}

// TestFormStreamContract_SourceFailureSpooledPath repeats the mid-stream
// source failure on the abnormal spool-to-disk path, proving the temporary
// backing file is removed even when the failure happens while the source is
// being drained into it.
func TestFormStreamContract_SourceFailureSpooledPath(t *testing.T) {
	parts := formtest.SeededParts(3)
	spooled := map[string]bool{"notes.txt": true}
	body := spoolWire(t, parts, spooled)

	// Fail inside the notes payload, which is being copied into a temp file.
	at := int64(indexOrDie(body, "notes.txt")) + 200

	cases := []struct {
		name     string
		memLimit formstream.DataSize
	}{
		{"memory", 0},
		{"disk", 256},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			before := formtest.TempFiles(t)
			fr := formtest.NewFailingReader(body, at, formtest.ErrSourceFailed)
			obs := runParse(t, fr, parts, parseOptions{spooledNames: spooled, memLimit: c.memLimit})
			if obs.err == nil {
				t.Fatal("Parse succeeded with a failing source on spooled path")
			}
			if !errors.Is(obs.err, formtest.ErrSourceFailed) {
				t.Fatalf("source cause lost on spooled %s path; chain:%s", c.name, formtest.DescribeError(obs.err))
			}
			formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))

			retry := runParse(t, byteChunks(body, formtest.OneByteChunks(len(body))), parts,
				parseOptions{spooledNames: spooled, memLimit: c.memLimit})
			assertSuccess(t, retry, parts, true)
		})
	}
}

// TestFormStreamContract_RecoverableFailureRetryStability is the
// mid-acceptance injection: the first run fails at a fixed point, then the
// identical input is parsed twice more successfully. All success runs must
// be byte-identical and order-identical, proving no cache/counter/temp
// residue from the failed call leaks into later calls.
func TestFormStreamContract_RecoverableFailureRetryStability(t *testing.T) {
	parts := formtest.SeededParts(5)
	wire := mustWire(t, parts)
	at := int64(indexOrDie(wire, "binary.bin") + 30)

	before := formtest.TempFiles(t)

	first := runParse(t, formtest.NewFailingReader(wire, at, formtest.ErrSourceFailed), parts, parseOptions{})
	if first.err == nil || !errors.Is(first.err, formtest.ErrSourceFailed) {
		t.Fatalf("injected run did not fail with the source cause: %v", first.err)
	}
	formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))

	var sigs []string
	for attempt := 0; attempt < 2; attempt++ {
		obs := runParse(t, byteChunks(wire, formtest.SeededSchedule(uint64(attempt+90), len(wire), 7)), parts, parseOptions{})
		assertSuccess(t, obs, parts, false)
		sigs = append(sigs, obs.signature())
	}
	if sigs[0] != sigs[1] {
		t.Fatalf("success results after failure were not stable:\n run1=%s\n run2=%s", sigs[0], sigs[1])
	}
}

func byteChunks(wire []byte, sch formtest.Schedule) *formtest.ChunkReader {
	return formtest.NewChunkReader(wire, sch, 0)
}

func indexOrDie(wire []byte, needle string) int {
	i := bytes.Index(wire, []byte(needle))
	if i < 0 {
		panic(fmt.Sprintf("fixture marker %q not found in wire", needle))
	}
	return i
}

func boundaryAt(wire []byte, nth int) int {
	marker := []byte("\r\n--" + formtest.Boundary)
	pos := 0
	for i := 0; i < nth; i++ {
		j := bytes.Index(wire[pos:], marker)
		if j < 0 {
			panic("boundary marker not found")
		}
		pos += j + len(marker)
	}
	return pos - len(marker) + 2
}

var _ = formstream.WithMaxMemSize
