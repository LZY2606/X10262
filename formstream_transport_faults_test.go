package formstream_test

// Fault-injection materials: recoverable source failure mid-stream,
// short-writing destinations, and context cancellation races. Each case
// records both caller-observable results and transport invariants
// (single error report, closed state, no temp-file or goroutine leak,
// and stable retry with identical input).
import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

var errInjectedSource = errors.New("formstream-test: injected source failure")
var errInjectedShortWrite = errors.New("formstream-test: injected short write")

// failingAfterReader serves the first failAfter bytes faithfully and
// then returns a stable sentinel error forever.
type failingAfterReader struct {
	data      []byte
	pos       int
	failAfter int
}

func (r *failingAfterReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAfter {
		return 0, errInjectedSource
	}
	n := copy(p, r.data[r.pos:r.failAfter])
	r.pos += n
	return n, nil
}

// countMatches reports the number of distinct error nodes matching
// target via errors.Is. A cause reported twice yields count >= 2.
func countMatches(err error, target error) int {
	// Leaf causes of an errors.Join tree, deduplicated by identity. A
	// single cause wrapped through several fmt.Errorf layers is one node;
	// the same cause reported twice yields two nodes.
	var leaves []error
	var collect func(error)
	collect = func(e error) {
		if e == nil {
			return
		}
		if je, ok := e.(interface{ Unwrap() []error }); ok {
			for _, c := range je.Unwrap() {
				collect(c)
			}
			return
		}
		if u := errors.Unwrap(e); u != nil {
			if _, joined := u.(interface{ Unwrap() []error }); joined {
				collect(u)
				return
			}
		}
		leaves = append(leaves, e)
	}
	collect(err)
	n := 0
	seen := map[error]bool{}
	for _, node := range leaves {
		if seen[node] {
			continue
		}
		seen[node] = true
		if errors.Is(node, target) {
			n++
		}
	}
	return n
}

func tempFileSnapshot() map[string]bool {
	m := map[string]bool{}
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return m
	}
	for _, e := range entries {
		if ok, _ := filepath.Match("formstream-*", e.Name()); ok {
			m[e.Name()] = true
		}
	}
	return m
}

func assertNoNewTempFiles(t *testing.T, before map[string]bool, tag string) {
	t.Helper()
	after := tempFileSnapshot()
	var leaked []string
	for f := range after {
		if !before[f] {
			leaked = append(leaked, f)
		}
	}
	sort.Strings(leaked)
	if len(leaked) > 0 {
		t.Errorf("%s: first violation: temp file leak after Parse returned: %v", tag, leaked)
	}
}

// goroutineSnapshot captures non-runtime background goroutines keyed by
// their user-code frame signature.
type goroutineSnapshot map[string]int

func snapshotGoroutines() goroutineSnapshot {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	s := goroutineSnapshot{}
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		key := signatureKey(g)
		if key == "" {
			continue
		}
		s[key]++
	}
	return s
}

func signatureKey(stack string) string {
	lines := strings.Split(stack, "\n")
	var frames []string
	for i := 1; i < len(lines); i++ {
		l := lines[i]
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if strings.HasPrefix(l, "\t") {
			continue // file path line
		}
		if strings.Contains(l, "created by ") {
			break
		}
		// Skip goroutines whose top user frame is part of the test
		// harness: parallel subtests must not count one another as leaks.
		if isTestFrame(l) {
			return ""
		}
		frames = append(frames, l)
	}
	joined := strings.Join(frames, "|")
	if strings.HasPrefix(joined, "runtime.") {
		return ""
	}
	return joined
}

func isTestFrame(frame string) bool {
	return strings.Contains(frame, ".test:") ||
		strings.Contains(frame, "_test.go:") ||
		strings.HasPrefix(frame, "testing.") ||
		strings.HasPrefix(frame, "github.com/mazrean/formstream_test.")
}

func assertNoGoroutineLeak(t *testing.T, before goroutineSnapshot, tag string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		after := snapshotGoroutines()
		var detail []string
		leaked := 0
		for sig, n := range after {
			if n > before[sig] {
				leaked += n - before[sig]
				detail = append(detail, fmt.Sprintf("+%d %s", n-before[sig], sig))
			}
		}
		if leaked == 0 {
			return
		}
		if time.Now().After(deadline) {
			sort.Strings(detail)
			t.Errorf("%s: first violation: %d goroutine(s) still running after Parse returned:\n%s",
				tag, leaked, strings.Join(detail, "\n"))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
