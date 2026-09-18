package streamflight

import (
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
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	buffer   int
	overflow Overflow
}

// WithBuffer sets how many values the channel queues. It is at least 1, which
// is also the default.
func WithBuffer(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.buffer = n }
}

// WithOverflow sets what a full queue does with an arriving value. The default
// is DropOldest.
func WithOverflow(o Overflow) SubscribeOption {
	return func(c *subscribeConfig) { c.overflow = o }
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
	// this subscriber gives up before Close needs the key's lock.
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
	return &Subscription[T]{
		C:        ch,
		fn:       fn,
		ch:       ch,
		overflow: overflow,
		closing:  make(chan struct{}),
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

// Close ends the subscription and releases its hold on the upstream. The last
// Close of a key stops the upstream, unless the Group lingers, and returns the
// error of stop. Close is idempotent and returns the same error every time.
func (s *Subscription[T]) Close() error {
	s.closeOnce.Do(func() {
		close(s.closing)
		s.closeErr = s.owner.leave(s)
	})
	return s.closeErr
}

// end finishes the subscription. Called once, under the key's lock.
func (s *Subscription[T]) end(err error) {
	s.err = err
	if s.ch != nil {
		close(s.ch)
	}
	close(s.done)
}
