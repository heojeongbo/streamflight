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

// Group shares one upstream per key among its subscribers.
//
// Set Source, and optionally the other fields, before first use and do not
// change them afterwards. A Group must not be copied after first use.
type Group[K comparable, T any] struct {
	// Source opens the upstream of a key. Required.
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
	// send is valid only during the call.
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

	mu      sync.Mutex
	flights map[K]*flight[K, T]
	closed  bool
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
		opt(&c)
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
	defer g.mu.Unlock()

	if g.closed {
		return nil
	}
	g.closed = true

	var errs []error
	for _, f := range g.flights {
		errs = append(errs, g.stopLocked(f, ErrGroupClosed))
	}
	return errors.Join(errs...)
}

func (g *Group[K, T]) subscribe(key K, s *Subscription[T]) error {
	f, err := g.acquire(key)
	if err != nil {
		return err
	}
	s.owner = f
	f.attach(s)
	return nil
}

// acquire returns the upstream of key with one more reference, opening it if
// key has none.
func (g *Group[K, T]) acquire(key K) (*flight[K, T], error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return nil, ErrGroupClosed
	}

	f := g.flights[key]
	if f != nil && f.ended() {
		// Stop an upstream that ended by itself before opening its successor,
		// so open never overtakes stop for the same key.
		g.stopLocked(f, nil)
		f = nil
	}

	if f == nil {
		f = newFlight(g, key)
		stop, err := g.Source(key, f)
		if g.Hooks.Opened != nil {
			g.Hooks.Opened(key, err)
		}
		if err != nil {
			// Nothing to stop, even if the Source ended it before failing.
			f.stopped = true
			return nil, err
		}
		f.stop = stop

		if g.flights == nil {
			g.flights = make(map[K]*flight[K, T])
		}
		g.flights[key] = f
	} else if f.timer != nil {
		// Back within Linger: keep it.
		f.timer.Stop()
		f.timer = nil
		f.gen++
	}

	f.refs++
	if g.Hooks.Joined != nil {
		g.Hooks.Joined(key, f.refs)
	}
	return f, nil
}

// release drops one reference to f and, if it was the last, stops f now or
// once it has lingered.
func (g *Group[K, T]) release(f *flight[K, T]) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	f.refs--
	if g.Hooks.Left != nil {
		g.Hooks.Left(f.key, f.refs)
	}
	if f.refs > 0 {
		return nil
	}

	if g.Linger > 0 && !f.stopped && !f.ended() {
		f.gen++
		gen := f.gen
		f.timer = time.AfterFunc(g.Linger, func() { g.expire(f, gen) })
		return nil
	}
	return g.stopLocked(f, nil)
}

// expire stops f once it has lingered, unless a subscriber came back since.
func (g *Group[K, T]) expire(f *flight[K, T], gen uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if f.gen == gen {
		g.stopLocked(f, nil)
	}
}

// stopLocked stops f once, ending what is still subscribed to it with reason.
// g.mu must be held.
func (g *Group[K, T]) stopLocked(f *flight[K, T], reason error) error {
	if f.stopped {
		return nil
	}
	f.stopped = true
	delete(g.flights, f.key)
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}

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
