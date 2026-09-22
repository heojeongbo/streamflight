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

	// ErrPollInterval is why opening a key failed when [Poll] was given an
	// interval that is not positive, which would spin instead of polling.
	ErrPollInterval = errors.New("streamflight: poll interval must be positive")
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
	//
	// Set ReplayFor instead when the answer depends on the key.
	Replay int

	// ReplayFor, if set, is Replay for one key, and replaces it. Use it when
	// only some keys are state a late subscriber has to be caught up on: a
	// topic published on change wants 1, a stream of events on the same Group
	// wants 0, and neither needs a Group of its own.
	//
	// It is called once per upstream, as the key is opened, with no Group lock
	// held and possibly at the same time as another key's. What it returns is
	// that upstream's for as long as the upstream lives, lingering included. It
	// must not call back into the Group.
	ReplayFor func(key K) int

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

	// Now, if set, is where a value gets the arrival time that
	// [Subscription.Latest] reports and [Subscription.Wait] waits past.
	// Defaults to time.Now.
	//
	// Set it to age a value from a test: whether a value is still current is
	// the caller's to decide, and deciding it is worth a test. It is called
	// while the key's values are being published, so keep it short and do not
	// call back into the Group. A key nobody samples never calls it.
	Now func() time.Time

	mu        sync.Mutex
	flights   map[K]*flight[K, T]
	closeDone chan struct{} // non-nil once a Close has started
}

// Hooks observe a Group. Every field is optional. None may call back into the
// Group, and all of them may be called concurrently for different keys. Joined
// and Left run under a lock shared by the whole Group, so their counts arrive
// in order; keep them short.
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

// SubscribeLatest joins key and keeps only its newest value, for a subscriber
// that samples on its own clock rather than being delivered to. Nothing is
// queued and nothing is called: each value replaces the one before it, and
// [Subscription.Latest] reads whatever is there when it is asked.
//
// Use it for state, where a reader wants the current answer whenever it looks:
// a handler on its own interval, a frame loop, a health check. A channel
// cannot do this, because receiving consumes — a reader that looks while the
// upstream is quiet finds an empty queue, not the value that is still true.
//
// It costs the key one stored value however many subscribers sample it, and
// costs a subscriber nothing per value. A key nobody samples stores nothing.
func (g *Group[K, T]) SubscribeLatest(key K) (*Subscription[T], error) {
	s := newSubscription[T](nil, nil, 0)
	if err := g.subscribe(key, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Close stops every upstream, ends every subscription with ErrGroupClosed and
// makes later Subscribe calls fail with it. It returns the errors of the stop
// funcs, joined. Close is idempotent.
//
// Close waits for an upstream another goroutine is opening or stopping, and a
// second Close waits for the first: once any Close returns, every upstream of
// the Group has been stopped.
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
			// Take the key first, then open it with the lock released. Holding
			// the key is what keeps anyone else from opening it; holding the
			// lock is not, and would stall every other key for the Source.
			f = newFlight(g, key)
			if g.flights == nil {
				g.flights = make(map[K]*flight[K, T])
			}
			g.flights[key] = f
			g.mu.Unlock()

			if err := g.open(key, f); err != nil {
				return nil, err
			}
			// Returned without re-reading the state: an upstream that ended
			// while opening belongs to this subscriber, which attach ends.
			return f, nil

		case f.st != live:
			// Another goroutine owns it. Wait for it to finish, then look
			// again: the key may be free, or held by a fresh upstream.
			w := f.waitLocked()
			g.mu.Unlock()
			<-w
			if err := f.openErr; err != nil {
				// Its open failed. Share the error rather than pile a second
				// attempt onto whatever made the first one fail.
				return nil, err
			}

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

// open runs the Source of f with no lock held and publishes what it returned.
// f already holds its key, so nobody else opens it and nobody stops it until
// this returns. On success f is live, with one reference held for the caller.
func (g *Group[K, T]) open(key K, f *flight[K, T]) (err error) {
	var stop func() error
	opened := false

	defer func() {
		g.mu.Lock()
		switch {
		case !opened:
			// Failed, or the Source panicked: give the key back, so the next
			// subscriber opens it again rather than waiting on it forever.
			delete(g.flights, key)
			f.st = dead
			f.openErr = err
		case g.closeDone != nil:
			// A Close began while this was opening. Publish it unreferenced so
			// that Close finds it and stops what was opened.
			f.st, f.stop = live, stop
			err = ErrGroupClosed
		default:
			f.st, f.stop = live, stop
			f.refs = 1
			if g.Hooks.Joined != nil {
				g.Hooks.Joined(key, 1)
			}
		}
		f.wakeLocked()
		g.mu.Unlock()
	}()

	// Before the Source, which can emit as soon as it has the Emitter, and
	// outside the lock, so a ReplayFor that takes its time holds up nobody.
	f.replay()

	stop, err = g.Source(key, f)
	if g.Hooks.Opened != nil {
		g.Hooks.Opened(key, err)
	}
	if err != nil {
		return err // nothing to stop, even if the Source ended it before failing
	}
	// Set last, and not from err: a panicking Source leaves err nil, and
	// publishing it live with no stop func would leak the upstream.
	opened = true
	return nil
}

// now is when a value arrived, from the Group's clock or the real one.
func (g *Group[K, T]) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
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
