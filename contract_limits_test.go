package formstream_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/formtest"
)

// TestFormStreamContract_BudgetExhaustion verifies that resource limits
// report their own causal sentinels (distinct from corruption or transport
// errors) and remain repeatable.
func TestFormStreamContract_BudgetExhaustion(t *testing.T) {
	parts := formtest.MinimalParts()
	wire := mustWire(t, parts)

	t.Run("too-many-parts", func(t *testing.T) {
		parser := formstream.NewParser(formtest.Boundary, formstream.WithMaxParts(1))
		err := parser.Parse(chunkAll(wire))
		if !errors.Is(err, formstream.ErrTooManyParts) {
			t.Fatalf("want ErrTooManyParts, got chain:%s", formtest.DescribeError(err))
		}
		if errors.Is(err, formstream.ErrTooLargeForm) {
			t.Fatal("part budget error wrongly classified as form size error")
		}
	})

	t.Run("too-many-headers", func(t *testing.T) {
		body := "--" + formtest.Boundary + "\r\n" +
			"Content-Disposition: form-data; name=\"a\"\r\n" +
			"X-One: 1\r\nX-Two: 2\r\n\r\nv\r\n" +
			"--" + formtest.Boundary + "--\r\n"
		parser := formstream.NewParser(formtest.Boundary, formstream.WithMaxHeaders(1))
		err := parser.Parse(strings.NewReader(body))
		if !errors.Is(err, formstream.ErrTooManyHeaders) {
			t.Fatalf("want ErrTooManyHeaders, got chain:%s", formtest.DescribeError(err))
		}
	})

	t.Run("too-large-form", func(t *testing.T) {
		before := formtest.TempFiles(t)
		parser := formstream.NewParser(formtest.Boundary, formstream.WithMaxMemSize(1))
		err := parser.Parse(chunkAll(wire))
		if !errors.Is(err, formstream.ErrTooLargeForm) {
			t.Fatalf("want ErrTooLargeForm, got chain:%s", formtest.DescribeError(err))
		}
		formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))
	})
}

// TestFormStreamContract_CorruptInput keeps corruption errors distinct from
// budget and transport errors, and proves a parser rejects the same corrupt
// body with the same classification on repeat.
func TestFormStreamContract_CorruptInput(t *testing.T) {
	good := mustWire(t, formtest.MinimalParts())

	cases := []struct {
		name string
		body []byte
	}{
		{"truncated-in-payload", good[:len(good)-40]},
		{"truncated-mid-boundary", good[:boundaryCut(good)]},
		{"garbage", []byte("this is not multipart at all")},
		{"wrong-boundary", bytesReplaceAll(good, []byte(formtest.Boundary), []byte("otherboundaryx"))},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err1 := formstream.NewParser(formtest.Boundary).Parse(chunkAll(c.body))
			if err1 == nil {
				t.Fatal("corrupt input was accepted")
			}
			if errors.Is(err1, formstream.ErrTooManyParts) ||
				errors.Is(err1, formstream.ErrTooManyHeaders) ||
				errors.Is(err1, formstream.ErrTooLargeForm) {
				t.Fatalf("corruption classified as a budget error; chain:%s", formtest.DescribeError(err1))
			}
			err2 := formstream.NewParser(formtest.Boundary).Parse(chunkAll(c.body))
			if err2 == nil {
				t.Fatal("corrupt input accepted on repeat")
			}
			if err1.Error() != err2.Error() {
				t.Fatalf("corruption rejection is not repeatable:\n first=%s\nsecond=%s",
					formtest.DescribeError(err1), formtest.DescribeError(err2))
			}
		})
	}
}

// TestFormStreamContract_DuplicateHookRejection exercises the existing
// rejection layer: a duplicate Register returns the typed value error, is
// repeatable, and leaves the hook map without a half-updated entry (the
// first hook still runs exactly once).
func TestFormStreamContract_DuplicateHookRejection(t *testing.T) {
	wire := mustWire(t, formtest.MinimalParts())

	parser := formstream.NewParser(formtest.Boundary)
	hits := 0
	if err := parser.Register("g.txt", func(io.Reader, formstream.Header) error {
		hits++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := parser.Register("g.txt", func(io.Reader, formstream.Header) error { return nil })
	var dup formstream.DuplicateHookNameError
	if !errors.As(err, &dup) {
		t.Fatalf("want DuplicateHookNameError, got %T chain:%s", err, formtest.DescribeError(err))
	}
	if dup.Name != "g.txt" {
		t.Fatalf("duplicate error lost hook name: %q", dup.Name)
	}
	// Repeatable rejection.
	err2 := parser.Register("g.txt", func(io.Reader, formstream.Header) error { return nil })
	if !errors.As(err2, &formstream.DuplicateHookNameError{}) {
		t.Fatalf("duplicate rejection not repeatable: %v", err2)
	}

	if err := parser.Parse(chunkAll(wire)); err != nil {
		t.Fatalf("parse after rejected registration failed: %v", err)
	}
	if hits != 1 {
		t.Fatalf("original hook ran %d times after a rejected duplicate", hits)
	}
}

// TestFormStreamContract_MemoryQuotaReturned proves successful streaming
// leaves no counter residue: reusing one Parser for repeated Parse calls
// keeps succeeding under a tight budget because spooled memory is returned.
func TestFormStreamContract_MemoryQuotaReturned(t *testing.T) {
	parts := []formtest.Part{
		{
			Name:        "f",
			FileName:    "f.txt",
			ContentType: "text/plain",
			Content:     []byte(strings.Repeat("q", 64)),
		},
	}
	spooled := map[string]bool{"f.txt": true}
	body := spoolWire(t, parts, spooled)

	parser := formstream.NewParser(formtest.Boundary,
		formstream.WithMaxMemSize(128), formstream.WithMaxMemFileSize(128))
	if err := parser.Register("f.txt", func(r io.Reader, _ formstream.Header) error {
		_, err := io.Copy(io.Discard, r)
		return err
	}, formstream.WithRequiredPart("marker")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := parser.Parse(formtest.NewChunkReader(body,
			formtest.SeededSchedule(uint64(i+1), len(body), 3), 0)); err != nil {
			t.Fatalf("run %d failed, memory counters were not returned to the pool: %v", i+1, err)
		}
	}
}

// TestFormStreamContract_NoStalePoolState poisons the sync.Pool indirectly:
// after a successful run, a later run with a *different* payload must never
// observe bytes from the earlier run, proving pooled buffers are reset.
func TestFormStreamContract_NoStalePoolState(t *testing.T) {
	mk := func(payload string) ([]formtest.Part, map[string]bool) {
		return []formtest.Part{
				{
					Name:        "f",
					FileName:    "f.txt",
					ContentType: "text/plain",
					Content:     []byte(payload),
				},
			},
			map[string]bool{"f.txt": true}
	}

	firstParts, spooled := mk(strings.Repeat("STALE-OLD-CONTENT", 32))
	firstBody := spoolWire(t, firstParts, spooled)
	first := runParse(t, chunkAll(firstBody), firstParts, parseOptions{spooledNames: spooled})
	assertSuccess(t, first, firstParts, true)

	secondParts, _ := mk("new-short")
	secondBody := spoolWire(t, secondParts, spooled)
	second := runParse(t, chunkAll(secondBody), secondParts, parseOptions{spooledNames: spooled})
	assertSuccess(t, second, secondParts, true)
	for _, f := range second.files {
		if strings.Contains(string(f.payload), "STALE") {
			t.Fatalf("payload contains stale bytes from a previous pooled buffer: %q", f.payload)
		}
	}
}

// TestFormStreamContract_EmptyFileAndUnicodeFilename explicitly pins the
// edge fixtures named in the contract: an empty file hook still fires once
// with an empty payload, and a Unicode filename round-trips byte-exact
// through the public Header accessor under every schedule.
func TestFormStreamContract_EmptyFileAndUnicodeFilename(t *testing.T) {
	parts := formtest.SeededParts(1)
	wire := mustWire(t, parts)

	schedules := []formtest.Schedule{
		{len(wire)},
		formtest.OneByteChunks(len(wire)),
		formtest.SeededSchedule(42, len(wire), 7),
	}
	for i, sch := range schedules {
		obs := runParse(t, formtest.NewChunkReader(wire, sch, 0), parts, parseOptions{})
		assertSuccess(t, obs, parts, false)

		byName := make(map[string]fileEvent, len(obs.files))
		for _, f := range obs.files {
			byName[f.fileName] = f
		}
		if e, ok := byName["empty.txt"]; !ok || len(e.payload) != 0 {
			t.Fatalf("schedule %d: empty file not observed empty: ok=%v len=%d", i, ok, len(e.payload))
		}
		u, ok := byName["テスト-🚴.txt"]
		if !ok {
			t.Fatalf("schedule %d: unicode filename hook did not fire", i)
		}
		if u.contentType != "text/plain; charset=utf-8" {
			t.Fatalf("schedule %d: unicode part content type = %q", i, u.contentType)
		}
	}
}

func chunkAll(b []byte) *formtest.ChunkReader {
	return formtest.NewChunkReader(b, formtest.Schedule{len(b)}, 0)
}

func boundaryCut(wire []byte) int {
	idx := strings.Index(string(wire), "\r\n--"+formtest.Boundary)
	if idx < 2 {
		return len(wire) / 2
	}
	return idx + 3 // cut through the "--B" fragment
}

func bytesReplaceAll(src, old, repl []byte) []byte {
	return []byte(strings.ReplaceAll(string(src), string(old), string(repl)))
}

var _ = fmt.Sprintf
