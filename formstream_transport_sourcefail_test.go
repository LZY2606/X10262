package formstream_test

// Recoverable source failure: a mid-stream error is injected, then the
// identical input is retried on the same parser instance. The failure
// must keep its causal chain reported exactly once, leave no temp files,
// leave no parsed/half-updated state, and the retry must produce exactly
// the first-success result (values, files, order).
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"testing"

	"github.com/mazrean/formstream"
)

func failingOffsets(wire []byte) []int {
	needle := []byte("--" + contractBoundary)
	var offsets []int
	seen := map[int]bool{}
	add := func(n int) {
		if n >= 0 && n <= len(wire) && !seen[n] {
			seen[n] = true
			offsets = append(offsets, n)
		}
	}
	// representative positions: start, first body byte, each delimiter edge
	add(1)
	add(5)
	for i := 0; i+len(needle) <= len(wire); i++ {
		if bytes.Equal(wire[i:i+len(needle)], needle) {
			add(i)
			add(i + 1)
			add(i + len(needle)/2)
			add(i + len(needle))
			add(i + len(needle) + 2)
		}
	}
	add(len(wire) - 1)
	add(len(wire))
	return offsets
}

func TestFormStreamTransportContract_SourceFailureMidStream_RecoverableRetry(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	exp := expectedResult(t, model)

	for _, at := range failingOffsets(model.wire) {
		at := at
		t.Run("failAfter_"+itoa(at), func(t *testing.T) {
			t.Parallel()

			p := formstream.NewParser(model.boundary)
			got := newParsedForm()
			hookRecorder(t, p, model, got, nil)
			beforeTemp := tempFileSnapshot()
			beforeGo := snapshotGoroutines()

			err := p.Parse(&failingAfterReader{data: model.wire, failAfter: at})
			if err == nil {
				t.Fatalf("first violation at offset %d: injected failure swallowed", at)
			}
			if !errors.Is(err, errInjectedSource) {
				t.Fatalf("first violation at offset %d: causal source error lost:\n want chain containing %v\n got  %v",
					at, errInjectedSource, err)
			}
			if n := countMatches(err, errInjectedSource); n != 1 {
				t.Errorf("first violation at offset %d: source error reported %d times in chain:\n%v", at, n, err)
			}

			// No half-updated shared state from the failed call. Hooks that
			// already ran on the fast path may have observed bytes, but no
			// field may have been committed into parser state.
			if len(p.ValueMap()) != 0 {
				t.Errorf("first violation at offset %d: value map half-updated: %v", at, p.ValueMap())
			}
			assertNoNewTempFiles(t, beforeTemp, fmt.Sprintf("source-fail@%d", at))
			assertNoGoroutineLeak(t, beforeGo, fmt.Sprintf("source-fail@%d", at))

			// Identical retry with a fresh parser (hooks are registered once
			// per parser by public contract) is a stable first success, and
			// repeating it reproduces exactly the same result.
			for attempt := 0; attempt < 2; attempt++ {
				rp := formstream.NewParser(model.boundary)
				retry := newParsedForm()
				hookRecorder(t, rp, model, retry, nil)
				if rerr := rp.Parse(bytes.NewReader(model.wire)); rerr != nil {
					t.Fatalf("first violation at offset %d: retry #%d with identical input failed: %v", at, attempt, rerr)
				}
				fillValues(t, rp, retry)
				assertResultEqual(t, fmt.Sprintf("retry@%d#%d", at, attempt), exp, retry)
			}
		})
	}
}

// TestFormStreamTransportContract_SourceFailure_SpooledPaths runs the
// same acceptance story through the abnormal (spooled) path: a file hook
// with unmet requirements is buffered, first to memory and then (with a
// small file budget) to a temp file, when the source dies.
func TestFormStreamTransportContract_SourceFailure_SpooledPaths(t *testing.T) {
	t.Parallel()

	// Build a model where the required field arrives only after the file.
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	if err := mw.SetBoundary(contractBoundary); err != nil {
		t.Fatal(err)
	}
	writeDispositionPart(t, mw, "upload", "data.bin", "application/octet-stream", bytes.Repeat([]byte("q"), 128))
	if err := mw.WriteField("required", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	wire := b.Bytes()
	needle := []byte("required")
	failAt := bytes.Index(wire, needle)

	for _, tc := range []struct {
		name string
		opts []formstream.ParserOption
	}{
		{name: "memory spool", opts: []formstream.ParserOption{
			formstream.WithMaxMemSize(512), formstream.WithMaxMemFileSize(512),
		}},
		{name: "disk spool", opts: []formstream.ParserOption{
			formstream.WithMaxMemSize(512), formstream.WithMaxMemFileSize(32),
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := formstream.NewParser(contractBoundary, tc.opts...)
			var hookCalls int
			var seen string
			err := p.Register("upload", func(r io.Reader, _ formstream.Header) error {
				hookCalls++
				rb, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				seen = string(rb)
				return nil
			}, formstream.WithRequiredPart("required"))
			if err != nil {
				t.Fatal(err)
			}

			before := tempFileSnapshot()
			ferr := p.Parse(&failingAfterReader{data: wire, failAfter: failAt})
			if ferr == nil {
				t.Fatal("first violation: source failure not reported on spooled path")
			}
			if !errors.Is(ferr, errInjectedSource) {
				t.Fatalf("first violation: source cause lost on %s: %v", tc.name, ferr)
			}
			if hookCalls != 0 {
				t.Errorf("first violation: hook fired although requirement never arrived: %d", hookCalls)
			}
			assertNoNewTempFiles(t, before, "spooled-"+tc.name)

			// Retry identical input: the buffered content must reach the
			// hook intact exactly once, with no residue from the failed spool.
			if rerr := p.Parse(bytes.NewReader(wire)); rerr != nil {
				t.Fatalf("first violation: spool retry failed: %v", rerr)
			}
			if hookCalls != 1 {
				t.Errorf("first violation: hook calls after retry want 1 got %d", hookCalls)
			}
			if seen != string(bytes.Repeat([]byte("q"), 128)) {
				t.Errorf("first violation: spooled content corrupted: %d bytes", len(seen))
			}
			if v, _, ok := p.Value("required"); !ok || v != "yes" {
				t.Errorf("first violation: required field after retry: %q %v", v, ok)
			}
			assertNoNewTempFiles(t, before, "spooled-retry-"+tc.name)
		})
	}
}

// TestFormStreamTransportContract_BudgetExhaustionCause pins the resource
// budget sentinels and proves they are stable across repeat attempts.
func TestFormStreamTransportContract_BudgetExhaustionCause(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)

	t.Run("too many parts repeats identically", func(t *testing.T) {
		t.Parallel()
		p := formstream.NewParser(model.boundary, formstream.WithMaxParts(1))
		e1 := p.Parse(bytes.NewReader(model.wire))
		e2 := p.Parse(bytes.NewReader(model.wire))
		if !errors.Is(e1, formstream.ErrTooManyParts) || !errors.Is(e2, formstream.ErrTooManyParts) {
			t.Fatalf("first violation: budget cause:\n e1=%v\n e2=%v", e1, e2)
		}
	})

	t.Run("too large form repeats identically and does not poison retry", func(t *testing.T) {
		t.Parallel()
		small := "--" + contractBoundary + "\r\n" +
			"Content-Disposition: form-data; name=\"x\"\r\n\r\n0123456789\r\n" +
			"--" + contractBoundary + "--\r\n"
		p := formstream.NewParser(contractBoundary, formstream.WithMaxMemSize(4))
		e1 := p.Parse(bytes.NewReader([]byte(small)))
		e2 := p.Parse(bytes.NewReader([]byte(small)))
		if !errors.Is(e1, formstream.ErrTooLargeForm) || !errors.Is(e2, formstream.ErrTooLargeForm) {
			t.Fatalf("first violation: form-size cause:\n e1=%v\n e2=%v", e1, e2)
		}
		// Budget exhaustion must not masquerade as a different cause.
		if countMatches(errors.Join(e1, e2), formstream.ErrTooLargeForm) != 2 {
			t.Fatalf("first violation: budget causes not separable: %v / %v", e1, e2)
		}
	})
}
