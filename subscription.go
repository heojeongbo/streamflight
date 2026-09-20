package streamflight

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Overflow is what a channel subscriber's queue does with a value that arrives
// while it is full.
type Overflow int

const (
	// DropOldest discards the oldest queued value to make room, so the queue
	// always holds the newest values. Right for state, where only the latest
	// matters. The default.
	DropOldest Overflow = iota

	// DropNewest discards the arriving value, so the queue keeps the oldest.
	DropNewest

	// Block waits until the subscriber makes room. Nothing is lost, but every
	// other subscriber of the key, and the Source itself, waits too. Closing
	// the subscription, ending the upstream or closing the Group releases it.
	Block

	// Evict closes the subscriber with ErrEvicted. Right when a gap would make
	// everything after it wrong, such as a stream of deltas.
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
// is DropOldest.
func WithOverflow(o Overflow) SubscribeOption {
	return func(c subscribeConfig) subscribeConfig { c.overflow = o; return c }
}

// Subscription is one subscriber of a key.
type Subscription[T any] struct {
	// C receives the values of a subscription made by Subscribe. It is closed
	// when the subscription ends, after which it still yields what was queued.
	// It is nil for a subscription made by SubscribeFunc.
	C <-chan T

	fn       func(T)
	ch       chan T
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

type owner[T any] interface {
	leave(s *Subscription[T]) error
	latest() (T, time.Time, bool)
	latestAfter(t time.Time) (T, time.Time, bool, <-chan struct{})
}

func newSubscription[T any](fn func(T), ch chan T, overflow Overflow) *Subscription[T] {
	var closing chan struct{}
	if fn == nil && overflow == Block {
		closing = make(chan struct{})
	}
	return &Subscription[T]{
		C:        ch,
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

// Dropped counts the values this subscriber lost to its Overflow policy.
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
// once somebody is watching for it. Whether a value is still current is the
// caller's to decide from at, because the answer depends on the key — silence
// on a topic published only when it changes means nothing changed, and on a
// sensor means the sensor is gone.
func (s *Subscription[T]) Latest() (v T, at time.Time, ok bool) {
	if s.fn != nil || s.ch != nil {
		panic("streamflight: Latest on a subscription that is delivered to")
	}
	return s.owner.latest()
}

// Wait blocks until the key has a value that arrived after t, and returns it.
// ok is false if ctx ends first or the subscription does.
//
// It is for reading back what you just wrote, where the value you want is not
// the one that is there: issue the write, note the time, and wait for a value
// newer than that rather than sampling the one the write has not reached yet.
// Pass a zero time to wait for the first value of all.
//
// Like [Subscription.Latest] it is valid only on a subscription from
// [Group.SubscribeLatest], and waits on nothing a delivery can hold.
func (s *Subscription[T]) Wait(ctx context.Context, t time.Time) (v T, at time.Time, ok bool) {
	if s.fn != nil || s.ch != nil {
		panic("streamflight: Wait on a subscription that is delivered to")
	}
	for {
		v, at, ok, newer := s.owner.latestAfter(t)
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
// returned, or why the subscription ended: [ErrEvicted], [ErrGroupClosed], or
// the error the upstream ended with. Values already queued when the upstream
// ended are sent before that. Use [Subscription.Err] to tell a client that went
// away from a clean end.
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
	if s.ch == nil {
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
// error of stop. Close is idempotent and returns the same error every time.
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
	if s.ch != nil {
		close(s.ch)
	}
}
