package streamflight

import (
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// flight is the upstream of one key: its Source's Emitter and the set of
// subscribers it delivers to.
type flight[K comparable, T any] struct {
	g    *Group[K, T]
	key  K
	stop func() error

	// quit is closed when the flight ends or is stopped, so an Emit waiting on
	// a Block subscriber gives up before the ending needs mu.
	quit     chan struct{}
	quitOnce sync.Once

	// Guarded by g.mu.
	refs    int
	gen     uint64 // bumped whenever a linger timer is armed or disarmed
	timer   *time.Timer
	stopped bool

	// ended mirrors done so the Group can tell whether this flight has ended
	// without waiting for a delivery that is holding mu. Written under mu.
	ended atomic.Bool

	// mu guards the fields below and is held for every delivery.
	mu     sync.Mutex
	subs   []*Subscription[T]
	ring   []T // the latest values, for Replay
	head   int // where the next value goes in ring
	count  int // how many values ring holds
	done   bool
	endErr error
}

type outcome int

const (
	accepted outcome = iota
	rejected
	evicted
)

func newFlight[K comparable, T any](g *Group[K, T], key K) *flight[K, T] {
	f := &flight[K, T]{
		g:    g,
		key:  key,
		quit: make(chan struct{}),
	}
	if g.Replay > 0 {
		f.ring = make([]T, g.Replay)
	}
	return f
}

func (f *flight[K, T]) Emit(v T) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return 0
	}
	if len(f.ring) > 0 {
		f.ring[f.head] = v
		f.head = (f.head + 1) % len(f.ring)
		f.count = min(f.count+1, len(f.ring))
	}

	n := 0
	for i := 0; i < len(f.subs); {
		s := f.subs[i]
		switch f.push(s, v, s.overflow) {
		case accepted:
			n++
		case evicted:
			f.subs = slices.Delete(f.subs, i, i+1)
			s.end(ErrEvicted)
			continue
		}
		i++
	}
	return n
}

func (f *flight[K, T]) End(err error) {
	if err == nil {
		err = io.EOF
	}
	f.unblock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.done {
		f.finish(err)
	}
}

// unblock lets an Emit waiting on a Block subscriber go.
func (f *flight[K, T]) unblock() {
	f.quitOnce.Do(func() { close(f.quit) })
}

// finish ends every subscriber with err and drops what is kept for Replay.
// f.mu must be held.
func (f *flight[K, T]) finish(err error) {
	// Before anything else: the Group reads this without taking f.mu, which is
	// what keeps a stalled delivery from stalling every other key. Do not
	// derive it from quit instead: End closes quit before calling finish, so a
	// quit-based check would let the Group end subscribers with a nil reason in
	// between, rather than with err.
	f.ended.Store(true)
	f.done = true
	f.endErr = err
	for _, s := range f.subs {
		s.end(err)
	}
	f.subs = nil
	f.ring = nil
	f.count = 0
}

// attach adds s, first sending it what Replay and Initial have for it.
func (f *flight[K, T]) attach(s *Subscription[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		s.end(f.endErr)
		return
	}

	// Never wait on a subscriber that cannot read yet: it is still inside
	// Subscribe. A queue shorter than what is sent keeps the newest.
	for i := range f.count {
		f.push(s, f.ring[(f.head-f.count+i+len(f.ring))%len(f.ring)], DropOldest)
	}
	if f.g.Initial != nil {
		// send borrows the key's lock, which is held only for this call. A
		// retained send would otherwise deliver without it, racing the whole
		// key and panicking on a queue that has since been closed.
		var live atomic.Bool
		live.Store(true)
		f.g.Initial(f.key, func(v T) {
			if !live.Load() {
				panic("streamflight: Group.Initial called send after returning")
			}
			f.push(s, v, DropOldest)
		})
		live.Store(false)
	}
	f.subs = append(f.subs, s)
}

func (f *flight[K, T]) leave(s *Subscription[T]) error {
	f.mu.Lock()
	if i := slices.Index(f.subs, s); i >= 0 {
		f.subs = slices.Delete(f.subs, i, i+1)
		s.end(ErrClosed)
	}
	f.mu.Unlock()

	return f.g.release(f)
}

// push delivers v to s under the given policy. f.mu must be held.
func (f *flight[K, T]) push(s *Subscription[T], v T, policy Overflow) outcome {
	if s.fn != nil {
		s.fn(v)
		return accepted
	}

	select {
	case s.ch <- v:
		return accepted
	default:
	}

	switch policy {
	case DropNewest:
		f.drop(s, v)
		return rejected

	case Block:
		select {
		case s.ch <- v:
			return accepted
		case <-s.closing:
		case <-f.quit:
		}
		return rejected

	case Evict:
		return evicted

	default: // DropOldest
		select {
		case old := <-s.ch:
			f.drop(s, old)
		default:
			// The subscriber made room itself in the meantime.
		}
		// Cannot block: a slot was just freed, by the receive above or by the
		// subscriber, and only this goroutine sends to the queue.
		s.ch <- v
		return accepted
	}
}

func (f *flight[K, T]) drop(s *Subscription[T], v T) {
	s.dropped.Add(1)
	if f.g.Hooks.Dropped != nil {
		f.g.Hooks.Dropped(f.key, v)
	}
}
