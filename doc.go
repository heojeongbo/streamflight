// Package streamflight shares one upstream stream per key among any number of
// subscribers.
//
// It is singleflight for streams. golang.org/x/sync/singleflight collapses
// concurrent calls for a key into one function call and hands every caller the
// same result. streamflight collapses concurrent subscriptions for a key into
// one upstream and hands every subscriber the same values, for as long as at
// least one subscriber remains.
//
// # Lifecycle
//
// A [Group] opens the upstream of a key through its [Source] when the first
// subscriber arrives. Later subscribers join it. When the last one leaves, the
// upstream is stopped, immediately or after [Group.Linger], and the key is
// forgotten: the next subscriber opens a fresh upstream.
//
// An upstream can also end by itself through [Emitter.End]. Every subscriber is
// then closed with the error it ended with, and the next subscriber opens a
// fresh upstream.
//
// # Delivery
//
// [Group.SubscribeFunc] calls a function for each value, on the goroutine that
// emitted it. It costs nothing per value beyond the call, but a slow function
// holds up every other subscriber of the key.
//
// [Group.Subscribe] queues values on a channel instead, and its [Overflow]
// policy decides what happens when the subscriber falls behind: keep the newest
// values ([DropOldest], the default), keep the oldest ([DropNewest]), wait for
// it ([Block]), or cut it off ([Evict]).
//
// [Group.SubscribeLatest] does not deliver at all. The key keeps its newest
// value and [Subscription.Latest] reads it, as often as the reader likes, for a
// subscriber on its own clock that wants the current answer whenever it looks.
// A channel cannot do that, because receiving consumes: a reader that looks
// while the upstream is quiet finds an empty queue rather than the value that
// is still true. The key stores it once however many subscribers sample it.
//
// [Subscription.Drain] is that channel pumped into a sink until a context ends,
// which is what a handler relaying one key to one client does. A write that can
// block belongs there rather than in SubscribeFunc, where it would hold up
// every other subscriber of the key. The sink is a func(T) error, so any
// protocol fits: a method value where the shape already matches, a two-line
// closure where it does not.
//
// A subscriber that joins a running upstream can first be sent what it missed:
// the latest values ([Group.Replay], or [Group.ReplayFor] when it depends on
// the key) or a snapshot of the current state ([Group.Initial]).
//
// A [Source] is written from whatever the upstream is. [Run] makes one out of a
// loop that produces values until its context is done; [Poll] out of a function
// called on an interval, for an upstream that is a repeated request rather than
// a subscription.
//
// # Guarantees
//
//   - One upstream per key: concurrent subscribers of a key open it once, and it
//     is stopped once.
//   - Open and stop are serialized: a key is never re-opened before its previous
//     upstream has been stopped.
//   - Values reach every subscriber of a key in the order they were emitted, and
//     a subscriber is never delivered to concurrently, even when the Source
//     emits from several goroutines.
//   - After [Subscription.Close] returns, its subscriber is never delivered to
//     again. A delivery in progress completes first.
//   - Values emitted after the upstream is stopped or has ended are dropped.
//
// # Rules
//
//   - Every subscriber of a key receives the same value. Treat it as read-only,
//     and copy it before mutating.
//   - Deliveries run on the emitting goroutine while holding the key's lock.
//     A function passed to SubscribeFunc, [Group.Initial] and [Hooks] must not
//     call back into the Group, and neither may Close be called from inside a
//     SubscribeFunc function: both deadlock.
//   - Open and stop run with no Group lock held, and different keys open and
//     stop at the same time. A [Source], [Group.ReplayFor], its stop func and
//     any goroutine they own may use the Group: they may subscribe to other
//     keys, and may close any subscription. They must not subscribe to their
//     own key or close the Group, because a key is held from the moment it
//     starts opening until its stop func has returned, so either call would
//     wait for itself.
//   - A stop func must return. Its key is unavailable until it does, and
//     [Group.Close] waits for it, but no other key is held up by it.
//   - Subscribe waits while another goroutine is opening or stopping the same
//     key. It never waits for another key's Source or stop func.
//   - The Joined and Left hooks run under a lock shared by the whole Group, so
//     that their counts are reported in order. Keep them short. Opened,
//     Stopped and Dropped run with no Group lock held, and every hook may run
//     concurrently for different keys.
//   - Always Close a Subscription, including one that has already ended. An
//     upstream is stopped only when all of its subscriptions are closed.
package streamflight
