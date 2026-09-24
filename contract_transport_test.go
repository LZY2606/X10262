package formstream_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/formtest"
)

// TestFormStreamContract_ModelArbitraryChunkBoundaries is the model-based
// regression: a fixed-seed body of fields and files must parse identically
// regardless of where transport chunk boundaries fall. It runs the fast
// (streaming) path and the abnormal (spool-to-memory/disk) path, and asserts
// not only decoded fields but the wire framing itself.
func TestFormStreamContract_ModelArbitraryChunkBoundaries(t *testing.T) {
	const seed = uint64(1)
	parts := formtest.SeededParts(seed)
	wire := mustWire(t, parts)

	schedules := []struct {
		name string
		sch  formtest.Schedule
	}{
		{"one-byte-chunks", formtest.OneByteChunks(len(wire))},
		{"single-chunk", formtest.Schedule{len(wire)}},
		{"seeded-random-n3", formtest.SeededSchedule(7, len(wire), 3)},
		{"seeded-random-n17", formtest.SeededSchedule(77, len(wire), 17)},
		{"seeded-random-n64", formtest.SeededSchedule(777, len(wire), 64)},
	}
	for _, bs := range formtest.BoundarySplitSchedules(wire) {
		schedules = append(schedules, struct {
			name string
			sch  formtest.Schedule
		}{"boundary-cross/" + bs.Name, bs.Schedule})
	}

	variants := []struct {
		name     string
		spooled  map[string]bool
		memLimit formstream.DataSize
	}{
		{"fast-path", nil, 0},
		{"slow-path-memory", map[string]bool{"notes.txt": true}, 0},
		{"slow-path-disk", map[string]bool{"notes.txt": true}, 1024},
	}

	for _, v := range variants {
		v := v
		t.Run(v.name, func(t *testing.T) {
			body := spoolWire(t, parts, v.spooled)
			if len(v.spooled) == 0 && !bytes.Equal(body, wire) {
				t.Fatal("wire model changed between fast/slow variants")
			}
			assertWireFraming(t, body, parts, len(v.spooled) > 0)

			for _, sc := range schedules {
				sc := sc
				t.Run(sc.name, func(t *testing.T) {
					r := formtest.NewChunkReader(body, sc.sch, 0)
					obs := runParse(t, r, parts, parseOptions{
						spooledNames: v.spooled,
						memLimit:     v.memLimit,
					})
					assertSuccess(t, obs, parts, len(v.spooled) > 0)
				})
			}
		})
	}
}

// TestFormStreamContract_StableUnderIrrelevantVariation proves the success
// result depends solely on the wire bytes: changing only chunk timing, the
// random chunk schedule seed, or the order in which hooks are exercised must
// produce the same order-sensitive signature, every time.
func TestFormStreamContract_StableUnderIrrelevantVariation(t *testing.T) {
	parts := formtest.SeededParts(11)
	wire := mustWire(t, parts)

	configs := []struct {
		name string
		sch  formtest.Schedule
		pace time.Duration
	}{
		{"whole", formtest.Schedule{len(wire)}, 0},
		{"one-byte", formtest.OneByteChunks(len(wire)), 0},
		{"one-byte-paced", formtest.OneByteChunks(len(wire)), 5 * time.Microsecond},
		{"random-a", formtest.SeededSchedule(101, len(wire), 5), 0},
		{"random-b", formtest.SeededSchedule(202, len(wire), 13), 0},
		{"random-c-paced", formtest.SeededSchedule(303, len(wire), 31), 20 * time.Microsecond},
	}

	var baseline string
	for i, c := range configs {
		obs := runParse(t, formtest.NewChunkReader(wire, c.sch, c.pace), parts, parseOptions{})
		assertSuccess(t, obs, parts, false)
		sig := obs.signature()
		if i == 0 {
			baseline = sig
			continue
		}
		if sig != baseline {
			t.Fatalf("observation changed under irrelevant variation %q:\n baseline=%s\n current=%s",
				c.name, baseline, sig)
		}
	}
}
