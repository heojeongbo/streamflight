package streamflight

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrClosed is why a subscription ended when it was closed by its owner.
	ErrClosed = errors.New("streamflight: subscription closed")

	// ErrEvicted is why a subscription ended when its queue overflowed under
	// the Evict policy.
	ErrEvicted = errors.New("streamflight: subscriber evicted for falling behind")

	// ErrGroupClosed is returned by Subscribe on a closed Group, and is why its
	// subscriptions ended when it was closed.
	ErrGroupClosed = errors.New("streamflight: group closed")
)

// state is where a flight is in its life.
//
// A flight occupies its key's slot in g.flights from the moment it starts
// opening until its stop func has returned. That occupancy, rather than a lock
// held across both, is what keeps a key from being re-opened before its
// previous upstream has been stopped. Three invariants follow from it:
//
//   - g.flights[k] is non-nil exactly while that flight is opening, live or
//     stopping. Whoever makes it dead deletes it.
//   - g.mu is a leaf: nothing is waited on while it is held, and the only user
//     code that runs under it is the Joined and Left hooks.
//   - f.stop is called by exactly one goroutine, the one whose live to stopping
//     claim succeeded.
type state uint8

const (
	opening  state = iota // the Source is running; nobody may join or stop it
	live                  // open succeeded: subscribers join and leave
	stopping              // the stop func is running, owned by one goroutine
	dead                  // terminal: the open failed, or the stop returned
)

// Group shares one upstream per key among its subscribers.
//
// Set Source, and optionally the other fields, before first use and do not
// change them afterwards. A Group must not be copied after first use.
type Group[K comparable, T any] struct {
	// Source opens the upstream of a key. Required: subscribing to a Group
	// without one panics.
	Source Source[K, T]

	// Replay is how many of the latest values a key remembers and delivers to
	// each subscriber that joins it, oldest first, before any live value.
	// Values emitted while the key lingers with no subscriber count too.
	//
	// Use it for state that is published only on change, where a late
	// subscriber would otherwise see nothing until the next change. Do not use
	// it for events: a replayed event is an old event delivered as a new one.
	Replay int

	// Initial, if set, is called for each subscriber that joins a key, after
	// Replay and before any live value, to send it values no other subscriber
	// receives, such as a snapshot of the current state for a stream of deltas.
	// send is valid only during the call: retaining it and calling it later
	// panics.
	//
	// No value is emitted during the call, but a value emitted right after it
	// may already be reflected in the snapshot, so deltas should be idempotent.
	Initial func(key K, send func(T))

	// Linger keeps an upstream running this long after its last subscriber
	// leaves, so a subscriber that comes back in time, such as a reloaded page,
	// reuses it instead of opening it again. Zero stops it immediately.
	Linger time.Duration

	// Hooks observe the Group, for logging and metrics.
	Hooks Hooks[K, T]

	mu        sync.Mutex
	flights   map[K]*flight[K, T]
	closeDone chan struct{} // non-nil once a Close has started
}

// Hooks observe a Group. Every field is optional. None may call back into the
// Group.
type Hooks[K comparable, T any] struct {
	// Opened is called after the Source of key was called, with its error.
	Opened func(key K, err error)

	// Stopped is called after the upstream of key was stopped, with the error
	// of its stop func.
	Stopped func(key K, err error)

	// Joined is called when a subscriber joins key, with how many it has now.
	Joined func(key K, n int)

	// Left is called when a subscriber of key closes, with how many remain.
	Left func(key K, n int)

	// Dropped is called with each value a subscriber of key loses to its
	// Overflow policy: under DropNewest the arriving value, which Emit did not
	// count; under DropOldest the queued value it displaced, which an earlier
	// Emit did count.
	Dropped func(key K, v T)
}

// Subscribe joins key, opening its upstream if it has no subscriber, and
// queues its values on the subscription's channel C.
func (g *Group[K, T]) Subscribe(key K, opts ...SubscribeOption) (*Subscription[T], error) {
	c := subscribeConfig{buffer: 1}
	for _, opt := range opts {
		c = opt(c)
	}

	s := newSubscription[T](nil, make(chan T, max(c.buffer, 1)), c.overflow)
	if err := g.subscribe(key, s); err != nil {
		return nil, err
	}
	return s, nil
}

// SubscribeFunc joins key, opening its upstream if it has no subscriber, and
// calls fn with each of its values on the goroutine that emitted it. fn must
// not block; see the package documentation.
func (g *Group[K, T]) SubscribeFunc(key K, fn func(T)) (*Subscription[T], error) {
	s := newSubscription[T](fn, nil, 0)
	if err := g.subscribe(key, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Close stops every upstream, ends every subscription with ErrGroupClosed and
// makes later Subscribe calls fail with it. It returns the errors of the stop
// funcs, joined. Close is idempotent.
func (g *Group[K, T]) Close() error {
	g.mu.Lock()
	if g.closeDone != nil {
		done := g.closeDone
		g.mu.Unlock()
		<-done // every upstream has been stopped once any Close returns
		return nil
	}
	done := make(chan struct{})
	g.closeDone = done // from here Subscribe fails
	g.mu.Unlock()
	defer close(done)

	var errs []error
	for {
		g.mu.Lock()
		var doomed []*flight[K, T]
		var w chan struct{}
		for _, f := range g.flights {
			if g.beginStop(f) {
				doomed = append(doomed, f)
			} else if w == nil {
				// Being opened or stopped elsewhere: wait for it rather than
				// leave it running.
				w = f.waitLocked()
			}
		}
		g.mu.Unlock()

		for _, f := range doomed {
			errs = append(errs, g.doStop(f, ErrGroupClosed))
		}
		if len(doomed) == 0 {
			if w == nil {
				return errors.Join(errs...)
			}
			<-w
		}
	}
}

func (g *Group[K, T]) subscribe(key K, s *Subscription[T]) error {
	if g.Source == nil {
		panic("streamflight: Group.Source is nil")
	}

	f, err := g.acquire(key)
	if err != nil {
		return err
	}
	s.owner = f
	f.attach(s)
	return nil
}

// acquire returns the upstream of key with one more reference, opening it if
// key has none and waiting if another goroutine is opening or stopping it.
func (g *Group[K, T]) acquire(key K) (*flight[K, T], error) {
	for {
		g.mu.Lock()
		// Re-checked every time round, so a waiter woken by Close never parks
		// again.
		if g.closeDone != nil {
			g.mu.Unlock()
			return nil, ErrGroupClosed
		}

		f := g.flights[key]
		switch {
		case f == nil:
			f = newFlight(g, key)
			stop, err := g.Source(key, f)
			if g.Hooks.Opened != nil {
				g.Hooks.Opened(key, err)
			}
			if err != nil {
				// Nothing to stop, even if the Source ended it before failing,
				// and nothing in the map to remove.
				f.st = dead
				g.mu.Unlock()
				return nil, err
			}
			f.stop = stop
			f.st = live
			if g.flights == nil {
				g.flights = make(map[K]*flight[K, T])
			}
			g.flights[key] = f
			f.refs++
			if g.Hooks.Joined != nil {
				g.Hooks.Joined(key, f.refs)
			}
			g.mu.Unlock()
			// Returned without re-reading the state: an upstream that ended
			// while opening belongs to this subscriber, which attach ends.
			return f, nil

		case f.st != live:
			// Another goroutine owns it. Wait for it to finish, then look
			// again: the key may be free, or held by a fresh upstream.
			w := f.waitLocked()
			g.mu.Unlock()
			<-w

		case f.ended.Load():
			// Stop an upstream that ended by itself before opening its
			// successor, so open never overtakes stop for the same key. Inline
			// on this goroutine, so the next pass opens straight afterwards.
			g.claimLocked(f)
			g.mu.Unlock()
			_ = g.doStop(f, nil)

		default:
			if f.timer != nil {
				// Back within Linger: keep it.
				f.timer.Stop()
				f.timer = nil
				f.gen++
			}
			f.refs++
			if g.Hooks.Joined != nil {
				g.Hooks.Joined(key, f.refs)
			}
			g.mu.Unlock()
			return f, nil
		}
	}
}

// claimLocked moves f from live to stopping, disarming its linger timer.
// g.mu must be held, and the caller must have seen f live under it.
func (g *Group[K, T]) claimLocked(f *flight[K, T]) {
	f.st = stopping
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
}

// beginStop claims the right to stop f, if it is still live. g.mu must be
// held. Exactly one caller ever gets true, which is what stops it once.
func (g *Group[K, T]) beginStop(f *flight[K, T]) bool {
	if f.st != live {
		return false
	}
	g.claimLocked(f)
	return true
}

// release drops one reference to f and, if it was the last, stops f now or
// once it has lingered.
func (g *Group[K, T]) release(f *flight[K, T]) error {
	g.mu.Lock()
	f.refs--
	if g.Hooks.Left != nil {
		g.Hooks.Left(f.key, f.refs)
	}
	if f.refs > 0 {
		g.mu.Unlock()
		return nil
	}

	if g.Linger > 0 && f.st == live && !f.ended.Load() {
		f.gen++
		gen := f.gen
		f.timer = time.AfterFunc(g.Linger, func() { g.expire(f, gen) })
		g.mu.Unlock()
		return nil
	}
	claimed := g.beginStop(f)
	g.mu.Unlock()

	if !claimed {
		return nil // already being stopped, by a Close, a takeover or the timer
	}
	// On this goroutine, so Close reports the stop error to its caller.
	return g.doStop(f, nil)
}

// expire stops f once it has lingered, unless a subscriber came back since.
func (g *Group[K, T]) expire(f *flight[K, T], gen uint64) {
	g.mu.Lock()
	// Two guards for two windows: gen covers a subscriber that came back before
	// this timer reached the lock, beginStop a Close or a takeover that claimed
	// the flight in the same window.
	claimed := f.gen == gen && g.beginStop(f)
	g.mu.Unlock()

	if claimed {
		_ = g.doStop(f, nil)
	}
}

// doStop stops f, ending what is still subscribed to it with reason, and then
// releases its key. It holds no lock while the stop func runs, and is reached
// only through a claim, so it runs exactly once per flight.
func (g *Group[K, T]) doStop(f *flight[K, T], reason error) error {
	defer func() {
		// Last, so that whoever takes the key next sees the whole stop, hooks
		// included, before it opens. Deferred so a panicking stop func frees
		// the key rather than wedging it.
		g.mu.Lock()
		delete(g.flights, f.key)
		f.st = dead
		f.wakeLocked()
		g.mu.Unlock()
	}()

	f.unblock()
	f.mu.Lock()
	if !f.done {
		f.finish(reason)
	}
	f.mu.Unlock()

	// Outside f.mu: stop may wait for an Emit that is waiting for f.mu.
	var err error
	if f.stop != nil {
		err = f.stop()
	}
	if g.Hooks.Stopped != nil {
		g.Hooks.Stopped(f.key, err)
	}
	return err
}
