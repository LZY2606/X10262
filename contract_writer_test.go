package formstream_test

import (
	"errors"
	"io"
	"testing"

	"github.com/mazrean/formstream"

	"github.com/mazrean/formstream/internal/formtest"
)

// TestFormStreamContract_TargetWriterShortWrite injects a writer that
// advertises a short write (n < len(p), nil). A correct hook must treat this
// as a failure rather than silently producing truncated output: the writer
// error surfaces exactly once, temp files are cleaned, and retrying with a
// healthy writer yields the complete payload.
func TestFormStreamContract_TargetWriterShortWrite(t *testing.T) {
	parts := formtest.SeededParts(6)
	wire := mustWire(t, parts)

	cases := []struct {
		name       string
		failAfter  int
		shortWrite bool
	}{
		{"error-first-write", 0, false},
		{"error-mid-notes", 120, false},
		{"error-near-end", len(wire) - 3, false},
		{"short-write-mid", 100, true},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			before := formtest.TempFiles(t)
			rec := formtest.NewRecorderWriter(c.failAfter, c.shortWrite, 0)

			obs := runParse(t, formtest.NewChunkReader(wire, formtest.OneByteChunks(len(wire)), 0), parts,
				parseOptions{hookWriter: func(string) io.Writer { return rec }})
			if obs.err == nil {
				t.Fatal("Parse succeeded despite failing/short target writer")
			}
			if !errors.Is(obs.err, formtest.ErrWriterFailed) {
				t.Fatalf("writer cause lost; chain:%s", formtest.DescribeError(obs.err))
			}
			if n := formtest.CountExactCause(obs.err, formtest.ErrWriterFailed); n != 1 {
				t.Fatalf("writer cause reported %d times, want 1; chain:%s", n, formtest.DescribeError(obs.err))
			}
			if obs.hookErrors != 1 {
				t.Fatalf("hook observed failure %d times, want exactly 1", obs.hookErrors)
			}
			if !c.shortWrite && rec.Accepted() > c.failAfter {
				t.Fatalf("writer accepted %d bytes after failure point %d", rec.Accepted(), c.failAfter)
			}
			formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))

			// Retry over the identical wire with a healthy writer.
			ok := formtest.NewRecorderWriter(-1, false, 0)
			retry := runParse(t, formtest.NewChunkReader(wire, formtest.OneByteChunks(len(wire)), 0), parts,
				parseOptions{hookWriter: func(string) io.Writer { return ok }})
			assertSuccess(t, retry, parts, false)
			if ok.Accepted() != totalFileBytes(parts) {
				t.Fatalf("healthy writer accepted %d bytes, model contains %d file bytes",
					ok.Accepted(), totalFileBytes(parts))
			}
		})
	}
}

// TestFormStreamContract_WriterErrorSpooledPath verifies the writer failure
// also propagates on the abnormal path and does not leave the spooled
// ReadCloser or backing temp file behind.
func TestFormStreamContract_WriterErrorSpooledPath(t *testing.T) {
	parts := formtest.SeededParts(8)
	spooled := map[string]bool{"notes.txt": true}
	body := spoolWire(t, parts, spooled)

	cases := []struct {
		name     string
		memLimit int64
	}{{"memory", 0}, {"disk", 128}}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			before := formtest.TempFiles(t)
			rec := formtest.NewRecorderWriter(20, false, 0)
			obs := runParse(t, formtest.NewChunkReader(body, formtest.SeededSchedule(31, len(body), 5), 0), parts,
				parseOptions{
					spooledNames: spooled,
					memLimit:     formstream.DataSize(c.memLimit),
					hookWriter:   func(string) io.Writer { return rec },
				})
			if obs.err == nil || !errors.Is(obs.err, formtest.ErrWriterFailed) {
				t.Fatalf("expected writer cause on spooled path; chain:%s", formtest.DescribeError(obs.err))
			}
			formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))
		})
	}
}
