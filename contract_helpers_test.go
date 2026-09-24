package formstream_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/formtest"
)

// fileEvent records what a file hook observed through the public API.
type fileEvent struct {
	name        string
	fileName    string
	contentType string
	payload     []byte
	order       int
}

// observation is the complete caller-visible result of one Parse run plus
// the invariants that witness multipart/streaming semantics.
type observation struct {
	err        error
	fields     map[string][]string
	files      []fileEvent
	hookErrors int
	// duplicateRegistration is the error returned by a duplicate Register.
	duplicateRegistration error
}

type parseOptions struct {
	// spooledNames forces the abnormal (spooled) path: hooks for these file
	// names are registered with WithRequiredPart("marker"), a trailing field
	// the model appends only for those runs.
	spooledNames map[string]bool
	// memLimit, when > 0, sets tiny budgets to drive the disk spill path.
	memLimit formstream.DataSize
	// hookWriter receives each file payload; failures from it abort Parse,
	// modeling a short/erroring target writer on the public hook surface.
	hookWriter func(name string) io.Writer
	// hookErr is returned from file hooks after observing payload.
	hookErr error
	// onFile is invoked synchronously inside the hook before it returns.
	onFile func(ev fileEvent, parser *formstream.Parser)
}

// runParse enters the library exclusively through the public surface:
// NewParser -> Register -> Parse. Every file name in the model gets a hook;
// fields are collected from Parser.Value/Values after Parse.
func runParse(t testing.TB, r io.Reader, parts []formtest.Part, opts parseOptions) observation {
	t.Helper()

	parser := formstream.NewParser(formtest.Boundary, parserLimits(opts)...)

	var (
		mu       sync.Mutex
		files    []fileEvent
		order    int
		errCount int
	)

	registered := make(map[string]struct{})
	for _, p := range parts {
		if !p.IsFile() {
			continue
		}
		if _, ok := registered[p.FileName]; ok {
			continue
		}
		registered[p.FileName] = struct{}{}

		var regOpts []formstream.RegisterOption
		if opts.spooledNames[p.FileName] {
			regOpts = append(regOpts, formstream.WithRequiredPart("marker"))
		}
		err := parser.Register(p.FileName, func(hr io.Reader, h formstream.Header) error {
			payload, copyErr := io.ReadAll(hr)
			if copyErr != nil {
				return fmt.Errorf("read hook payload for %q: %w", p.FileName, copyErr)
			}
			if opts.hookWriter != nil {
				w := opts.hookWriter(p.FileName)
				if w != nil {
					if _, err := io.Copy(w, bytes.NewReader(payload)); err != nil {
						mu.Lock()
						errCount++
						mu.Unlock()
						return fmt.Errorf("copy payload into target for %q: %w", p.FileName, err)
					}
				}
			}
			mu.Lock()
			ev := fileEvent{
				name:        h.Name(),
				fileName:    h.FileName(),
				contentType: h.ContentType(),
				payload:     append([]byte(nil), payload...),
				order:       order,
			}
			order++
			files = append(files, ev)
			mu.Unlock()
			if opts.onFile != nil {
				opts.onFile(ev, parser)
			}
			if opts.hookErr != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return opts.hookErr
			}
			return nil
		}, regOpts...)
		if err != nil {
			return observation{err: fmt.Errorf("register %q: %w", p.FileName, err), duplicateRegistration: err}
		}
	}

	err := parser.Parse(r)

	fields := make(map[string][]string)
	seenField := make(map[string]struct{})
	for _, p := range parts {
		if p.IsFile() || p.Name == "" {
			continue
		}
		if _, ok := seenField[p.Name]; ok {
			continue
		}
		seenField[p.Name] = struct{}{}
		if vs, ok := parser.Values(p.Name); ok {
			for _, v := range vs {
				s, _ := v.Unwrap()
				fields[p.Name] = append(fields[p.Name], s)
			}
		}
	}

	mu.Lock()
	defer mu.Unlock()
	return observation{
		err:        err,
		fields:     fields,
		files:      append([]fileEvent(nil), files...),
		hookErrors: errCount,
	}
}

func parserLimits(opts parseOptions) []formstream.ParserOption {
	var out []formstream.ParserOption
	if opts.memLimit > 0 {
		out = append(out,
			formstream.WithMaxMemSize(opts.memLimit),
			formstream.WithMaxMemFileSize(opts.memLimit),
		)
	}
	return out
}

// spoolWire appends the trailing marker field that satisfies abnormal-path
// requirements, keeping the model otherwise identical.
func spoolWire(t testing.TB, parts []formtest.Part, spooled map[string]bool) []byte {
	t.Helper()
	if len(spooled) == 0 {
		return mustWire(t, parts)
	}
	withMarker := append(append([]formtest.Part(nil), parts...), formtest.Part{
		Name:    "marker",
		Content: []byte("done"),
	})
	return mustWire(t, withMarker)
}

func mustWire(t testing.TB, parts []formtest.Part) []byte {
	t.Helper()
	wire, err := formtest.BuildWire(parts)
	if err != nil {
		t.Fatalf("build wire model: %v", err)
	}
	return wire
}

func expectedFields(parts []formtest.Part, spooled bool) map[string][]string {
	fields := make(map[string][]string)
	for _, p := range parts {
		if p.IsFile() {
			continue
		}
		fields[p.Name] = append(fields[p.Name], string(p.Content))
	}
	if spooled {
		fields["marker"] = []string{"done"}
	}
	return fields
}

// assertSuccess checks the caller-visible model (field order/duplicates,
// file order, filenames, content types and byte-exact payloads) and reports
// the first violation with full context.
func assertSuccess(t testing.TB, obs observation, parts []formtest.Part, spooled bool) {
	t.Helper()

	if obs.err != nil {
		t.Fatalf("Parse returned error, expected success; chain:%s", formtest.DescribeError(obs.err))
	}

	wantFields := expectedFields(parts, spooled)
	if len(obs.fields) != len(wantFields) {
		t.Fatalf("field key count mismatch: want %d keys %v, got %d keys %v",
			len(wantFields), sortedKeys(wantFields), len(obs.fields), sortedKeys(obs.fields))
	}
	for name, wantVals := range wantFields {
		gotVals := obs.fields[name]
		if len(gotVals) != len(wantVals) {
			t.Fatalf("field %q value count mismatch: want %d (%v), got %d (%v)",
				name, len(wantVals), wantVals, len(gotVals), gotVals)
		}
		for i := range wantVals {
			if gotVals[i] != wantVals[i] {
				t.Fatalf("field %q[%d] mismatch:\n want %q\n got  %q", name, i, wantVals[i], gotVals[i])
			}
		}
	}

	var wantFiles []formtest.Part
	for _, p := range parts {
		if p.IsFile() {
			wantFiles = append(wantFiles, p)
		}
	}
	if len(obs.files) != len(wantFiles) {
		t.Fatalf("file hook count mismatch: want %d, got %d", len(wantFiles), len(obs.files))
	}
	for i, want := range wantFiles {
		got := obs.files[i]
		if got.order != i {
			t.Fatalf("file %d (%s) observed out of order: hook order=%d", i, want.FileName, got.order)
		}
		if got.name != want.Name {
			t.Fatalf("file %d: name want %q, got %q", i, want.Name, got.name)
		}
		if got.fileName != want.FileName {
			t.Fatalf("file %d: filename want %q, got %q", i, want.FileName, got.fileName)
		}
		if got.contentType != want.ContentType {
			t.Fatalf("file %d (%s): content-type want %q, got %q", i, want.FileName, want.ContentType, got.contentType)
		}
		if !bytes.Equal(got.payload, want.Content) {
			t.Fatalf("file %d (%s): payload mismatch (want %d bytes, got %d bytes); first diff at %s",
				i, want.FileName, len(want.Content), len(got.payload), firstDiff(want.Content, got.payload))
		}
	}
}

// signature computes a stable order-sensitive summary of an observation so
// that runs differing only in irrelevant scheduling can be compared cheaply.
func (obs observation) signature() string {
	var b strings.Builder
	fmt.Fprintf(&b, "err=%v|", obs.err)
	for _, k := range sortedKeys(obs.fields) {
		fmt.Fprintf(&b, "%s=%v;", k, obs.fields[k])
	}
	for _, f := range obs.files {
		fmt.Fprintf(&b, "%s:%s:%s:%x;", f.name, f.fileName, f.contentType, f.payload)
	}
	return b.String()
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func firstDiff(a, b []byte) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("byte %d (want 0x%02x, got 0x%02x)", i, a[i], b[i])
		}
	}
	if len(a) != len(b) {
		return fmt.Sprintf("byte %d (length boundary)", n)
	}
	return "none"
}

func totalFileBytes(parts []formtest.Part) int {
	n := 0
	for _, p := range parts {
		if p.IsFile() {
			n += len(p.Content)
		}
	}
	return n
}

const leakWait = 5 * time.Second
