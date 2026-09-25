package streamflight

import (
	"context"
	"time"
)

// Source opens the upstream of key. It is called once per upstream, by the
// first subscriber of the key, and must deliver values through e until stop is
// called. A nil stop means there is nothing to stop.
//
// It runs with no Group lock held, so it may take as long as opening really
// takes without holding up another key, and it may use the Group itself to
// build one stream out of others; see the package documentation for the two
// calls it must not make.
//
// Values emitted before the subscriber opening the key is attached, which is
// after Source returns, reach it only through [Group.Replay] or
// [Group.ReplayFor]. A subscriber from [Group.SubscribeLatest] is the
// exception: a key it opens keeps the newest of them for it.
type Source[K comparable, T any] func(key K, e Emitter[T]) (stop func() error, err error)

// Emitter is a Source's handle on the subscribers of its key. It is safe for
// concurrent use.
type Emitter[T any] interface {
	// Emit delivers v to every subscriber of the key and returns how many of
	// them accepted it: a function subscriber and a sampler always do, and a
	// channel subscriber does when v was queued. It returns once every
	// subscriber has been delivered to, which under [Block] can mean waiting.
	// What Replay and Initial send a subscriber as it joins is counted by no
	// Emit.
	Emit(v T) int

	// End ends the upstream by itself. Every subscriber is closed with err, or
	// io.EOF if err is nil, and the next subscriber of the key opens a fresh
	// upstream. The stop func is still called: once every subscriber has been
	// closed, or when the next subscriber of the key arrives, whichever is
	// first. Emit and End after the first End do nothing.
	End(err error)
}

// Run makes a Source out of a function that produces values until its context
// is done, such as a poller or a loop reading a connection.
//
// run is started on its own goroutine when the key is opened. Stopping the key
// cancels ctx and waits for run to return, so run must return once ctx is
// done: a read that does not take a context is unblocked by closing what it
// reads, as in context.AfterFunc(ctx, func() { conn.Close() }). If run returns
// while ctx is still live, the upstream ends with the returned error; see
// [Emitter.End].
//
// Setup that can fail, or that depends on the key, belongs in a Source that
// does it and then returns Run(run)(key, e). The two fail differently: an
// error from that Source fails Subscribe, while an error from run never does.
// It ends the upstream, and the subscription Subscribe returned ends with it.
// The same goes for [Poll], whose interval can come from the key this way.
func Run[K comparable, T any](run func(ctx context.Context, key K, e Emitter[T]) error) Source[K, T] {
	return func(key K, e Emitter[T]) (func() error, error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			// A no-op once the key is stopping, which is when ctx is done.
			e.End(run(ctx, key, e))
		}()

		return func() error {
			cancel()
			<-done
			return nil
		}, nil
	}
}

// Poll makes a [Source] out of a function called on an interval, for an
// upstream that is a repeated request rather than a subscription: an HTTP
// backend, a query, a value sampled at a rate.
//
// tick is called once as soon as the key is opened, so a subscriber has
// something on open instead of after one interval, and then interval after the
// previous call returned. Not on a [time.Ticker]: a Ticker keeps the tick a
// slow call missed and fires again at once, so a call that outruns interval
// leaves the loop running back to back with no idle at all. Scheduling from the
// return guarantees interval of quiet between calls, whatever a call costs.
//
// The first tick can run before the subscriber that opened the key is
// attached. A channel or function subscriber that opens it is sure of that
// first value only with [Group.Replay] of at least 1, which holds it until the
// subscriber is attached. A subscriber from [Group.SubscribeLatest] needs
// nothing: a key it opens keeps what its Source emits while opening.
//
// A tick that returns an error ends the upstream with it, closing every
// subscriber; the next subscriber opens a fresh one. To make a failure a value
// instead, which is usually right for a backend expected to come back, emit it
// and return nil: ending the stream would turn one outage into a reconnect
// loop. A tick with nothing to report simply does not emit.
//
// The interval belongs to the upstream, as does anything a tick remembers
// between calls, such as the last value it sent. Both live in a Source that
// closes over them, since a Source runs once per upstream and Poll returns one.
//
// Opening a key whose interval is not positive fails with [ErrPollInterval]
// rather than spinning.
func Poll[K comparable, T any](
	interval time.Duration,
	tick func(ctx context.Context, key K, e Emitter[T]) error,
) Source[K, T] {
	run := Run(func(ctx context.Context, key K, e Emitter[T]) error {
		// Zero, so the first tick is on open; reset after the work, so the
		// interval is quiet time rather than a deadline the work can miss.
		t := time.NewTimer(0)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
			if err := tick(ctx, key, e); err != nil {
				return err
			}
			t.Reset(interval)
		}
	})

	return func(key K, e Emitter[T]) (func() error, error) {
		if interval <= 0 {
			return nil, ErrPollInterval
		}
		return run(key, e)
	}
}
