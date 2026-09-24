package formstream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mazrean/formstream/internal/formtest"
)

// TestFormStreamContract_ContextCancelDeterministic cancels the transport at
// fixed byte offsets through a pipe. After Parse returns, the producer
// goroutine must have exited and the pipe write side must be closed
// exactly once; the context cause must be reported exactly once.
func TestFormStreamContract_ContextCancelDeterministic(t *testing.T) {
	parts := formtest.SeededParts(4)
	wire := mustWire(t, parts)

	points := []int{1, len(wire) / 4, len(wire) / 2, len(wire) - 10}
	for _, at := range points {
		at := at
		t.Run("cancel-after-"+itoa(at), func(t *testing.T) {
			before := formtest.TempFiles(t)
			beforeG := formtest.CountGoroutinesIn(t, "internal/formtest")

			pc, _ := formtest.NewPipeCancel(context.Background(), wire, 0, at)
			obs := runParse(t, pc.Reader(), parts, parseOptions{})
			if obs.err == nil {
				t.Fatal("Parse succeeded after transport cancellation, expected an error")
			}
			if !errors.Is(obs.err, context.Canceled) {
				t.Fatalf("context cause lost from error chain; chain:%s", formtest.DescribeError(obs.err))
			}
			if n := formtest.CountExactCause(obs.err, context.Canceled); n != 1 {
				t.Fatalf("cancellation cause reported %d times, want 1; chain:%s", n, formtest.DescribeError(obs.err))
			}

			if err := pc.Wait(leakWait); err != nil {
				t.Fatalf("producer goroutine leaked after cancel: %v", err)
			}
			if !pc.Closed() {
				t.Fatal("pipe write side was not marked closed after cancel")
			}
			if err := pc.ReadClosedAfterWait(); err != nil {
				t.Fatalf("pipe close state violated: %v", err)
			}
			if got := formtest.CountGoroutinesIn(t, "internal/formtest"); got > beforeG {
				t.Fatalf("formtest goroutines grew after cancel: before=%d after=%d", beforeG, got)
			}
			formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))
		})
	}
}

// TestFormStreamContract_CancelRacePaced races externally-paced cancellation
// against chunk delivery across many seeds. Every run must either fail with
// context.Canceled (and close/clean up everything) or deliver the full model;
// a hang, a duplicated cause or leaked goroutine fails the test.
func TestFormStreamContract_CancelRacePaced(t *testing.T) {
	parts := formtest.MinimalParts()
	wire := mustWire(t, parts)

	for seed := uint64(0); seed < 24; seed++ {
		seed := seed
		t.Run("race-"+itoa64(seed), func(t *testing.T) {
			before := formtest.TempFiles(t)

			pc, cancel := formtest.NewPipeCancel(context.Background(), wire, time.Duration(seed%3)*time.Microsecond, -1)
			done := make(chan observation, 1)
			go func() {
				done <- runParse(t, pc.Reader(), parts, parseOptions{})
			}()

			// Cancel at a seed-dependent delay so the race covers prefix,
			// mid-payload and near-close positions.
			time.Sleep(time.Duration(seed*37) * time.Microsecond)
			cancel()

			var obs observation
			select {
			case obs = <-done:
			case <-time.After(leakWait):
				t.Fatal("Parse did not return after cancellation")
			}

			if err := pc.Wait(leakWait); err != nil {
				t.Fatalf("producer goroutine leaked: %v", err)
			}
			if !pc.Closed() {
				t.Fatal("pipe not closed after external cancel")
			}

			switch {
			case obs.err == nil:
				assertSuccess(t, obs, parts, false)
			case errors.Is(obs.err, context.Canceled):
				if n := formtest.CountExactCause(obs.err, context.Canceled); n != 1 {
					t.Fatalf("cancellation cause reported %d times; chain:%s", n, formtest.DescribeError(obs.err))
				}
				formtest.AssertNoNewTempFiles(t, before, formtest.TempFiles(t))
			default:
				t.Fatalf("unexpected error class after cancel race; chain:%s", formtest.DescribeError(obs.err))
			}
		})
	}
}

func itoa(n int) string { return itoa64(uint64(n)) }
func itoa64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
