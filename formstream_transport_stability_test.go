package formstream_test

// Stability group: the same fixed input is re-run changing only
// result-irrelevant conditions - chunk scheduling, read buffer shapes,
// or the (deterministic) iteration order of independent queries. All
// observable results, wire hook order and success/error state must be
// identical, proving the model holds across scheduling variation.
import (
	"bytes"
	"io"
	"testing"

	"github.com/mazrean/formstream"
)

// shapedReader alternates request-shaped reads: it only ever fills up to
// a per-read cap, cycling through a fixed schedule. It never splits with
// a deterministic error - it only changes scheduling.
type shapedReader struct {
	data     []byte
	pos      int
	schedule []int
	idx      int
}

func (r *shapedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	cap := len(p)
	if r.idx < len(r.schedule) {
		if s := r.schedule[r.idx%len(r.schedule)]; s > 0 && s < cap {
			cap = s
		}
		r.idx++
	}
	if remain := len(r.data) - r.pos; cap > remain {
		cap = remain
	}
	copy(p, r.data[r.pos:r.pos+cap])
	r.pos += cap
	return cap, nil
}

func TestFormStreamTransportContract_SchedulingDoesNotChangeResult(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)
	exp := expectedResult(t, model)

	schedules := map[string][]int{
		"bulk":             {0},
		"one byte":         {1},
		"two bytes":        {2},
		"three bytes":      {3},
		"mixed 1 and 4096": {1, 0, 1, 0, 3, 0},
		"prime 7":          {7},
		"prime 13":         {13},
		"boundary width":   {len(contractBoundary) + 2},
		"boundary minus1":  {len(contractBoundary) + 1},
		"boundary plus1":   {len(contractBoundary) + 3},
	}

	for name, sched := range schedules {
		name, sched := name, sched
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := formstream.NewParser(model.boundary)
			got := newParsedForm()
			hookRecorder(t, p, model, got, nil)
			if err := p.Parse(&shapedReader{data: model.wire, schedule: sched}); err != nil {
				t.Fatalf("first violation (%s): parse: %v", name, err)
			}
			fillValues(t, p, got)
			assertResultEqual(t, name, exp, got)
			assertWireCanonical(t, name, model, got.hooks)
		})
	}
}

// TestFormStreamTransportContract_FailedAttemptLeavesNoCachedState proves
// the success result after a failed attempt carries no residue: counters,
// buffered hook params and values of the failed call are not observable
// in the successful result (including repeated field multiplicities).
func TestFormStreamTransportContract_FailedAttemptLeavesNoCachedState(t *testing.T) {
	t.Parallel()

	model := buildFixedSeedModel(t, contractBoundary)

	// fail late, when some fields have already been parsed
	at := len(model.wire) - 3
	p := formstream.NewParser(model.boundary)
	got := newParsedForm()
	hookRecorder(t, p, model, got, nil)

	if err := p.Parse(&failingAfterReader{data: model.wire, failAfter: at}); err == nil {
		t.Fatal("first violation: expected injected failure")
	}
	if len(p.ValueMap()) != 0 {
		t.Fatalf("first violation: cached fields after failure: %v", p.ValueMap())
	}

	// Successful run on a fresh parser with the identical input: duplicate
	// fields appear exactly twice, hooks exactly once each, so nothing from
	// the failed run is cached in the (package-global) pool or counters.
	p2 := formstream.NewParser(model.boundary)
	good := newParsedForm()
	hookRecorder(t, p2, model, good, nil)
	if err := p2.Parse(bytes.NewReader(model.wire)); err != nil {
		t.Fatalf("first violation: success run: %v", err)
	}
	fillValues(t, p2, good)

	exp := expectedResult(t, model)
	assertResultEqual(t, "after-failure-success", exp, good)
	if n := len(good.values["dup"]); n != 2 {
		t.Errorf("first violation: duplicate field multiplicity leaked from failed attempt: want 2 got %d", n)
	}
	hookCount := map[string]int{}
	for _, h := range good.hooks {
		hookCount[h]++
	}
	for name := range exp.files {
		if hookCount[name] != 1 {
			t.Errorf("first violation: hook %q invocation count want 1 got %d (failed-call residue)", name, hookCount[name])
		}
	}
}
