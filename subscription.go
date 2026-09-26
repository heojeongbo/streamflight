package streamflight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Overflow is what a channel subscriber's queue does with a value that arrives
// while it is full.
//
// What Replay and Initial send a subscriber as it joins is never waited for,
// since the subscriber is still inside Subscribe and cannot read yet. A queue
// too short for it keeps the newest, whatever the policy, and counts what it
// displaced in Dropped, except under Evict, which ends the subscription with
// ErrEvicted instead: Subscribe then returns it already ended.
type Overflow int

const (
	// DropOldest discards the oldest queued value to make room, so the queue
	// always holds the newest values. Right for state, where only the latest
	// matters. The default.
	DropOldest Overflow = iota

	// DropNewest discards the arriving value, so the queue keeps the oldest.
	DropNewest

	// Block waits until the subscriber makes room. Nothing emitted while it is
	// subscribed is lost, but every other subscriber of the key, and the Source
	// itself, waits too. Closing the subscription, ending the upstream or
	// closing the Group releases it: the value it was waiting to deliver is not
	// delivered, nor is anything emitted after that before the subscriber is
	// ended, and none of them counts as dropped.
	Block

	// Evict closes the subscriber with ErrEvicted. Right when a gap would make
	// everything after it wrong, such as a stream of deltas, and so a gap in
	// what it is sent on joining counts too.
	Evict
)

// SubscribeOption configures a channel subscription.
//
// It takes and returns a config by value rather than by pointer so the config
// stays on the stack: passing its address to a func value would escape it.
type SubscribeOption func(subscribeConfig) subscribeConfig

type subscribeConfig struct {
	buffer   int
	overflow Overflow
}

// WithBuffer sets how many values the channel queues. It is at least 1, which
// is also the default.
func WithBuffer(n int) SubscribeOption {
	return func(c subscribeConfig) subscribeConfig { c.buffer = n; return c }
}

// WithOverflow sets what a full queue does with an arriving value. The default
// is DropOldest. It panics on a value that is not one of the four, rather than
// let a mistyped policy lose values some other way than the one asked for.
func WithOverflow(o Overflow) SubscribeOption {
	if o < DropOldest || o > Evict {
		panic(fmt.Sprintf("streamflight: WithOverflow with an unknown Overflow %d", int(o)))
	}
	return func(c subscribeConfig) subscribeConfig { c.overflow = o; return c }
}

// Subscription is one subscriber of a key.
type Subscription[T any] struct {
	// C receives the values of a subscription made by Subscribe. It is closed
	// when the subscription ends, after which it still yields what was queued.
	// It is nil for a subscription made by SubscribeFunc or SubscribeLatest,
	// and receiving from it then blocks forever.
	C <-chan T

	kind     kind
	fn       func(T) // set for called
	ch       chan T  // set for queued
	overflow Overflow
	owner    owner[T]

	// closing is closed as soon as Close starts, so a Block delivery waiting on
	// this subscriber gives up before Close needs the key's lock. Only a Block
	// delivery waits, so it is nil for every other subscription.
	closing chan struct{}
	done    chan struct{}
	err     error // written once, before done is closed

	dropped atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// kind is how a subscription receives its values. It is said once, by the
// method that made the subscription, rather than read back from which of fn
// and ch happen to be nil.
type kind uint8

const (
	queued  kind = iota // Subscribe: values queue on ch
	called              // SubscribeFunc: fn is called with each value
	sampled             // SubscribeLatest: the key keeps its newest value
)

type owner[T any] interface {
	leave(s *Subscription[T]) error
	latest() (T, time.Time, bool)
	latestAfter(t time.Time) (T, time.Time, bool, <-chan struct{})
}

func newSubscription[T any](k kind, fn func(T), ch chan T, overflow Overflow) *Subscription[T] {
	var closing chan struct{}
	if k == queued && overflow == Block {
		closing = make(chan struct{})
	}
	return &Subscription[T]{
		C:        ch,
		kind:     k,
		fn:       fn,
		ch:       ch,
		overflow: overflow,
		closing:  closing,
		done:     make(chan struct{}),
	}
}

// Done is closed when the subscription ends: when it is closed, when its
// upstream ends or its Group is closed, or when it is evicted.
func (s *Subscription[T]) Done() <-chan struct{} {
	return s.done
}

// Err is nil while the subscription is live. Once Done is closed it says why:
// ErrClosed, ErrEvicted, ErrGroupClosed, or the error the upstream ended with.
func (s *Subscription[T]) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// Dropped counts the values this subscriber lost to a full queue: under
// DropNewest those it refused, under DropOldest those it displaced, and, under
// any policy but Evict, what Replay and Initial sent it on joining that did
// not fit. See [Overflow].
func (s *Subscription[T]) Dropped() uint64 {
	return s.dropped.Load()
}

// Latest returns the newest value of the key, when it arrived, and whether
// there is one yet. It is valid only on a subscription from
// [Group.SubscribeLatest], and reads without waiting for a delivery, so a
// subscriber that has fallen behind never holds a sampler up.
//
// There is nothing until the first value emitted after the first sampler of
// the key joined, the same as any latch: a key remembers its newest value only
// once somebody is watching for it. A sampler that opened the key is watching
// from the start, so it also has what the Source emitted while opening.
// Whether a value is still current is the caller's to decide from at, because
// the answer depends on the key — silence on a topic published only when it
// changes means nothing changed, and on a sensor means the sensor is gone.
//
// Once the subscription has ended, Latest goes on reading the upstream it was
// on: its newer values for as long as it runs, for other subscribers or while
// it lingers, and its last value once it has ended or been stopped. Done or
// Err says whether this subscription is still live. It never sees the fresh
// upstream that the key's next subscriber opens.
func (s *Subscription[T]) Latest() (v T, at time.Time, ok bool) {
	if s.kind != sampled {
		panic("streamflight: Latest on a subscription that is delivered to")
	}
	return s.owner.latest()
}

// Wait blocks until the key has a value that arrived after the given time, and
// returns the newest there is. ok is false if ctx ends first, or if the
// subscription has ended with no such value. A zero time returns the current
// value, or waits for the first if there is none yet.
//
// It is for reading back what you just wrote, where the value that is there is
// the one the write has not reached yet. The time is an arrival time on the
// clock of [Group.Now], so take it from Latest rather than from time.Now:
//
//	_, seen, _ := s.Latest()
//	write()
//	v, _, ok := s.Wait(ctx, seen)
//
// Where seen is taken decides what counts. On a key published only when it
// changes, take it before the write, as here: the change can arrive before the
// write returns, and a time taken after it would wait past the change. On a
// key published periodically, take it after the write returns, so that what
// arrived before the write returned does not count. Either way, a value newer
// than seen need not show the write: one sampled before the write can still
// arrive after it. When the write shows in the value itself, wait until it
// does, passing each value's arrival time to the next Wait: every call returns
// a value newer than the last, and the newest there is, so a loop never sees
// one twice and never falls behind, though it may skip values that were
// already replaced.
//
// Like [Subscription.Latest] it is valid only on a subscription from
// [Group.SubscribeLatest], and waits on nothing a delivery can hold.
func (s *Subscription[T]) Wait(ctx context.Context, after time.Time) (v T, at time.Time, ok bool) {
	if s.kind != sampled {
		panic("streamflight: Wait on a subscription that is delivered to")
	}
	for {
		v, at, ok, newer := s.owner.latestAfter(after)
		if ok {
			return v, at, true
		}
		select {
		case <-newer:
		case <-ctx.Done():
			return v, at, false
		case <-s.done:
			return v, at, false
		}
	}
}

// Drain sends every value of the subscription with send, until ctx is done or
// the subscription ends. It is the body of a handler that relays one key to one
// client.
//
// It returns nil when ctx is done and nil when the upstream ended cleanly,
// which are the two ordinary ways a relay finishes: the client went away, or
// there is nothing left to send. Otherwise it returns the first error send
// returned, or why the subscription ended: [ErrClosed] when another goroutine
// closed it, [ErrEvicted], [ErrGroupClosed], or the error the upstream ended
// with. Values already queued when the upstream ended are sent before that.
// Use [Subscription.Err] to tell a client that went away from a clean end.
//
// send runs on the caller's goroutine, one value at a time, so it may block: no
// other subscriber of the key waits for it, and this subscription's [Overflow]
// policy decides what falling behind costs. That is why a network write belongs
// here rather than in [Group.SubscribeFunc], where it would hold up everyone.
// send is any protocol's write: a method value where the shape already fits, a
// two-line closure where it does not.
//
// Drain does not close the subscription. The caller still owns it, and may
// Drain it again. It panics on a subscription that has no channel to drain,
// which is any made by [Group.SubscribeFunc] or [Group.SubscribeLatest].
func (s *Subscription[T]) Drain(ctx context.Context, send func(T) error) error {
	if s.kind != queued {
		panic("streamflight: Drain on a subscription with no channel")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-s.ch:
			if !ok {
				if err := s.Err(); !errors.Is(err, io.EOF) {
					return err
				}
				return nil
			}
			if err := send(v); err != nil {
				return err
			}
		}
	}
}

// Close ends the subscription and releases its hold on the upstream. The last
// Close of a key stops the upstream, unless the Group lingers, and returns the
// error of stop when this Close is what stopped it. Close is idempotent and
// returns the same error every time.
func (s *Subscription[T]) Close() error {
	s.closeOnce.Do(func() {
		if s.closing != nil {
			close(s.closing)
		}
		s.closeErr = s.owner.leave(s)
	})
	return s.closeErr
}

// end finishes the subscription. Called once, under the key's lock.
func (s *Subscription[T]) end(err error) {
	s.err = err
	// Before the queue. A reader that ranges over C asks Err why it ended as
	// soon as C closes, and closing done second would let it find no reason at
	// all. A reader selecting on both sees the end while values are still
	// queued instead, and C goes on yielding them.
	close(s.done)
	if s.kind == queued {
		close(s.ch)
	}
}
