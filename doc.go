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
// A subscriber that joins a running upstream can first be sent what it missed:
// the latest values ([Group.Replay]) or a snapshot of the current state
// ([Group.Initial]).
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
//   - Open, stop and every hook except Dropped run under a lock shared by the
//     whole Group. Keep them short.
//   - Always Close a Subscription, including one that has already ended. An
//     upstream is stopped only when all of its subscriptions are closed.
package streamflight
