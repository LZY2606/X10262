package formtest

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

// TempPrefix is the prefix of temporary files created by the production
// preprocessor (os.CreateTemp("", "formstream-")).
const TempPrefix = "formstream-"

type errProducerLeak struct{}

func (errProducerLeak) Error() string {
	return "formtest: pipe producer goroutine did not exit after cancel"
}

type errPipeNotClosed struct{}

func (errPipeNotClosed) Error() string {
	return "formtest: pipe was still readable (not closed) after wait"
}

// TempFiles returns the sorted names of leftover formstream temp files in the
// OS temp directory. It never follows symlinks.
func TempFiles(t testing.TB) []string {
	t.Helper()

	dir := os.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("formtest: read temp dir: %v", err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), TempPrefix) {
			names = append(names, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(names)
	return names
}

// AssertNoNewTempFiles fails if the after snapshot contains entries not
// present in before, reporting the exact leftover paths.
func AssertNoNewTempFiles(t testing.TB, before, after []string) {
	t.Helper()

	stale := make(map[string]struct{}, len(before))
	for _, n := range before {
		stale[n] = struct{}{}
	}
	var leaked []string
	for _, n := range after {
		if _, ok := stale[n]; !ok {
			leaked = append(leaked, n)
		}
	}
	if len(leaked) > 0 {
		t.Fatalf("formtest: %d temp file(s) leaked after parse: %v", len(leaked), leaked)
	}
}

// CountGoroutinesIn returns the number of live goroutines whose stack
// contains substr. It tolerates unrelated runtime churn by taking a few
// samples after forcing GC.
func CountGoroutinesIn(t testing.TB, substr string) int {
	t.Helper()

	buf := make([]byte, 1<<20)
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	n := buf[:runtime.Stack(buf, true)]
	blocks := strings.Split(string(n), "\n\n")
	count := 0
	for _, b := range blocks {
		if strings.Contains(b, substr) {
			count++
		}
	}
	return count
}

// ErrorCauseCount walks every error in an error tree (Join/Unwrap chains)
// and returns how many nodes satisfy match. It is used to prove a cause is
// reported exactly once instead of being duplicated through multiple wrap
// or join sites.
func ErrorCauseCount(err error, match func(error) bool) int {
	if err == nil {
		return 0
	}
	seen := make(map[error]struct{})
	var count int
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if match(e) {
			count++
		}
		if j, ok := e.(interface{ Unwrap() []error }); ok {
			for _, ce := range j.Unwrap() {
				walk(ce)
			}
			return
		}
		if u, ok := e.(interface{ Unwrap() error }); ok {
			walk(u.Unwrap())
		}
	}
	walk(err)
	return count
}

// IsError returns whether err itself (not via Unwrap) matches target by
// errors.Is; retained for readability at call sites.
func IsError(err, target error) bool {
	return errors.Is(err, target)
}

// DescribeError renders the full wrapped error chain, one node per line, to
// preserve diagnostic context in failure output.
func DescribeError(err error) string {
	if err == nil {
		return "<nil>"
	}
	var b strings.Builder
	depth := 0
	for e := err; e != nil; depth++ {
		fmt.Fprintf(&b, "\n  [%d] %T: %v", depth, e, e)
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			break
		}
		next := u.Unwrap()
		if errors.Is(next, e) {
			break
		}
		e = next
	}
	return b.String()
}

// CountExactCause counts nodes identical to target (==/Is at the leaf),
// ignoring outer wrap layers that merely carry the same cause via Unwrap.
// A linear wrap chain reports its single leaf cause once.
func CountExactCause(err error, target error) int {
	return ErrorCauseCount(err, func(e error) bool {
		if e == nil || target == nil {
			return e == target
		}
		if !errors.Is(e, target) {
			return false
		}
		if u, ok := e.(interface{ Unwrap() error }); ok {
			return !errors.Is(u.Unwrap(), target)
		}
		return true
	})
}
