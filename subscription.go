package streamflight

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
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
// Drain it again. It panics on a subscription from [Group.SubscribeFunc], which
// has no channel to drain.
func (s *Subscription[T]) Drain(ctx context.Context, send func(T) error) error {
	if s.ch == nil {
		panic("streamflight: Drain on a SubscribeFunc subscription")
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
