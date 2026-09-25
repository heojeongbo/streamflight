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
	// A channel subscriber whose queue is shorter than what is replayed keeps
	// the newest; see [Overflow]. A subscriber from SubscribeLatest is sent
	// nothing: it reads what the key keeps instead.
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
	// that upstream's for as long as the upstream lives, lingering included.
	// Like a Source, it may use the Group but not subscribe to its own key.
	ReplayFor func(key K) int

	// Initial, if set, is called for each subscriber that joins a key, after
	// Replay and before any live value, to send it values no other subscriber
	// receives, such as a snapshot of the current state for a stream of deltas.
	// send is valid only during the call: retaining it and calling it later
	// panics.
	//
	// No value is emitted during the call, but a value emitted right after it
	// may already be reflected in the snapshot, so deltas should be idempotent.
	//
	// It runs under the key's lock, like a delivery. What it sends to a channel
	// subscriber whose queue is too short follows the rule in [Overflow]: under
	// Evict the subscription ends with ErrEvicted rather than start from a
	// snapshot with a gap in it. What it sends a sampler is not kept.
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
	// the caller's to decide, and deciding it is worth a test. Under
	// testing/synctest there is no need to: time.Now is already the bubble's
	// clock, and time.Sleep ages a value. It is called under the key's lock
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
// in order; keep them short. Dropped runs under the key's lock, inside the
// delivery that dropped the value. A hook that panics fails the call that
// reported it, and nothing more.
type Hooks[K comparable, T any] struct {
	// Opened is called after the Source of key was called, with its error.
	Opened func(key K, err error)

	// Stopped is called after the upstream of key was stopped, with the error
	// of its stop func.
	Stopped func(key K, err error)

	// Joined is called when a subscriber joins key, with how many it has now.
	// It is called before the subscriber is sent what Replay and Initial have
	// for it, so it is not a sign that the subscriber can receive yet.
	Joined func(key K, n int)

	// Left is called when a subscriber of key closes, with how many remain.
	Left func(key K, n int)

	// Dropped is called with each value a subscriber of key loses to a full
	// queue: under DropNewest the arriving value, which Emit did not count;
	// under DropOldest the queued value it displaced, which an earlier Emit did
	// count unless it was sent on joining, by Replay or Initial.
	Dropped func(key K, v T)
}

// Subscribe joins key, opening its upstream if it has no subscriber, and
// queues its values on the subscription's channel C.
//
// With no options the queue holds one value and a full queue drops its oldest
// ([DropOldest]): right for state, where only the latest matters, and wrong for
// events, which want [WithBuffer] and perhaps another [Overflow]. Relaying the
// values to a client is [Subscription.Drain].
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
// calls fn with each of its values on the goroutine that emitted it. What
// Replay and Initial have for it is delivered first, on the calling goroutine,
// before SubscribeFunc returns: a fn that refers to the returned subscription
// finds it nil for those. fn must not block; see the package documentation.
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
// costs a subscriber no more per value than a step through a loop. A key
// nobody samples stores nothing.
//
// A key a sampler opens keeps what its Source emits while opening, such as the
// current state it read on subscribing. The first sampler of a key someone
// else opened starts with nothing until the next value: Replay keeps no
// arrival times, so it cannot seed one.
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
	return g.closeAll()
}

// closeAll stops every upstream of a closing Group, waiting for any another
// goroutine is opening or stopping. A stop func or Stopped hook that panics
// fails Close, but only once the rest are stopped too: Subscribe already
// fails, so nobody else would stop one that finishes opening afterwards.
func (g *Group[K, T]) closeAll() error {
	finished := false
	defer func() {
		if !finished {
			_ = g.closeAll()
		}
	}()

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

		errs = g.stopAll(doomed, errs)
		if len(doomed) == 0 {
			if w == nil {
				finished = true
				return errors.Join(errs...)
			}
			<-w
		}
	}
}

// stopAll stops the flights Close has claimed. A stop func or Stopped hook
// that panics fails Close, but only once the rest are stopped too: nobody
// else will stop an upstream Close has claimed.
func (g *Group[K, T]) stopAll(doomed []*flight[K, T], errs []error) []error {
	next := 0
	defer func() {
		if next < len(doomed) {
			g.stopAll(doomed[next:], nil)
		}
	}()
	for next < len(doomed) {
		f := doomed[next]
		next++
		errs = append(errs, g.doStop(f, ErrGroupClosed))
	}
	return errs
}

func (g *Group[K, T]) subscribe(key K, s *Subscription[T]) error {
	if g.Source == nil {
		panic("streamflight: Group.Source is nil")
	}

	f, err := g.acquire(key, s.fn == nil && s.ch == nil)
	if err != nil {
		return err
	}
	s.owner = f
	// attach runs the caller's code: Initial, a SubscribeFunc function being
	// caught up, Hooks.Dropped. If that panics, the subscription is never
	// returned, so give back the reference it would have held.
	attached := false
	defer func() {
		if !attached {
			_ = g.release(f)
		}
	}()
	f.attach(s)
	attached = true
	return nil
}

// acquire returns the upstream of key with one more reference, opening it if
// key has none and waiting if another goroutine is opening or stopping it.
// sampler says whether the caller samples rather than being delivered to.
func (g *Group[K, T]) acquire(key K, sampler bool) (*flight[K, T], error) {
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
			// A sampler that opens the key keeps what the Source emits while
			// opening, which would otherwise arrive before attach tells the key
			// to keep it. Set before f is published or the Source has its
			// Emitter, so nobody can read it yet.
			f.wanted = sampler
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
			// Reported before anything changes, so a hook that panics takes
			// no reference with it.
			g.reportLocked(g.Hooks.Joined, key, f.refs+1)
			if f.timer != nil {
				// Back within Linger: keep it.
				f.timer.Stop()
				f.timer = nil
				f.gen++
			}
			f.refs++
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
	opened, reported := false, false

	defer func() {
		g.mu.Lock()
		// Whatever happens below, a Joined hook that panics included: the
		// Group must not stay locked, and whoever waits on the key must hear.
		defer func() {
			f.wakeLocked()
			g.mu.Unlock()
		}()
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
		case !reported:
			// Opened panicked, so the subscriber opening it gets no
			// subscription. Publish it unreferenced all the same: the next
			// subscriber to leave it, or Close, stops what was opened.
			f.st, f.stop = live, stop
		default:
			f.st, f.stop = live, stop
			// Before the reference, so a hook that panics takes none with it.
			if g.Hooks.Joined != nil {
				g.Hooks.Joined(key, 1)
			}
			f.refs = 1
		}
	}()

	// Before the Source, which can emit as soon as it has the Emitter, and
	// outside the lock, so a ReplayFor that takes its time holds up nobody.
	f.replay()

	stop, err = g.Source(key, f)
	if err != nil {
		if g.Hooks.Opened != nil {
			g.Hooks.Opened(key, err)
		}
		return err // nothing to stop, even if the Source ended it before failing
	}
	// Set before Opened, and not from err: a panicking Source leaves err nil,
	// and publishing it live with no stop func would leak the upstream, while a
	// panicking Opened must not keep what the Source opened from being stopped.
	opened = true
	if g.Hooks.Opened != nil {
		g.Hooks.Opened(key, nil)
	}
	reported = true
	return nil
}

// reportLocked calls a Joined or Left hook, which runs under g.mu so that the
// counts it reports arrive in order. g.mu must be held. A hook that panics
// fails the call it ran in, but does not take g.mu with it: that would wedge
// every key of the Group rather than fail one call.
func (g *Group[K, T]) reportLocked(hook func(K, int), key K, n int) {
	if hook == nil {
		return
	}
	returned := false
	defer func() {
		if !returned {
			g.mu.Unlock()
		}
	}()
	hook(key, n)
	returned = true
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
	g.reportLocked(g.Hooks.Left, f.key, f.refs)
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
