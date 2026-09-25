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
// A Group need not have many keys. One upstream shared by whoever wants it,
// opened by the first and stopped after the last, is a Group[struct{}, T]
// subscribed to with struct{}{}: the reference count comes with it.
//
// # Delivery
//
// Which way to subscribe depends on what is done with each value. Sending it
// somewhere that can block, such as a client connection, is [Group.Subscribe]
// and [Subscription.Drain]. Short work that never blocks, such as counting or
// updating a map, is [Group.SubscribeFunc]. Reading the current value on the
// reader's own clock is [Group.SubscribeLatest].
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
// is still true. The key stores it once however many subscribers sample it,
// and [Subscription.Wait] blocks for one that arrived after a time Latest
// returned, which is how a caller reads back what it has just written.
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
// the key) or a snapshot of the current state ([Group.Initial]). A function
// subscriber is sent them inside SubscribeFunc, on the calling goroutine,
// before it returns. A sampler keeps neither: Initial is still called for it,
// but it reads what the key keeps.
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
//   - Once a key has a sampler, a value emitted to it becomes its newest before
//     it is delivered to any subscriber. A SubscribeFunc function woken by a
//     value therefore reads that same value from [Subscription.Latest], never
//     the one before it, which is what lets one subscription carry the state
//     and another the edge. A channel reader, which reads later, reads it or a
//     newer one. This holds for values delivered as they are emitted, not for
//     what Replay sends a subscriber as it joins, which Latest may already
//     have moved past.
//   - After [Subscription.Close] returns, its subscriber is never delivered to
//     again. A delivery in progress completes first.
//   - Values emitted after the upstream is stopped or has ended are dropped.
//   - Keys are independent: opening or stopping one key never waits for
//     another, and neither does a subscriber that has fallen behind, except
//     inside [Group.Close], which stops keys one at a time, and while a Joined
//     or Left hook runs under the lock the whole Group shares.
//
// # Rules
//
//   - Every subscriber of a key receives the same value. Treat it as read-only,
//     and copy it before mutating.
//   - Deliveries run while holding the key's lock, on the emitting goroutine or,
//     for what a subscriber is sent as it joins, on the subscribing one.
//     [Group.Initial], [Group.Now] and the Dropped hook run under that lock too.
//     From there it is safe to read [Subscription.Latest], Err and Dropped,
//     which never wait. A SubscribeFunc function or Initial may also Emit on
//     another key, which is how a stream derived from this one is fed, unless
//     what that key delivers leads back to this one. It is not safe to
//     subscribe or to Close a subscription on any key, since either can run a
//     Source or stop func that needs this key, nor to call [Subscription.Wait],
//     Emit or End on the same key, or to close the Group: each waits on the
//     lock the delivery holds, and deadlocks.
//   - Open and stop run with no Group lock held, and different keys open and
//     stop at the same time. A [Source], [Group.ReplayFor], its stop func and
//     any goroutine they own may use the Group: they may subscribe to other
//     keys, and may close any subscription. They must not subscribe to their
//     own key or close the Group, because a key is held from the moment it
//     starts opening until its stop func has returned, so either call would
//     wait for itself.
//   - A stop func must return. Its key is unavailable until it does, and
//     [Group.Close], which stops keys one at a time, waits for it before it
//     stops the next. Outside Close, no other key is held up by it.
//   - Subscribe waits while another goroutine is opening or stopping the same
//     key. It never waits for another key's Source or stop func.
//   - The Joined and Left hooks run under a lock shared by the whole Group, so
//     that their counts are reported in order. Keep them short, and do not
//     call back into the Group from any hook. Opened and Stopped run with no
//     lock held, Dropped under the key's lock as a delivery does, and every
//     hook may run concurrently for different keys.
//   - Always Close a Subscription, including one that has already ended. An
//     upstream is stopped when all of its subscriptions are closed (after
//     [Group.Linger], unless it has ended), when the Group is closed, or, once
//     it has ended by itself, when the next subscriber of its key arrives to
//     open a fresh one.
//   - A panic in the caller's code, whether a hook, [Group.Initial],
//     [Group.Now], a SubscribeFunc function, a Source or a stop func, fails the
//     call it ran in: the Group is not left locked, and nobody is left waiting
//     on a key. A key whose Opened, Joined or Left hook panicked may go on
//     running until its next subscriber leaves it or the Group is closed, and
//     whatever a Source or stop func had started and not stopped when it
//     panicked, the Group never stops. On a goroutine the package starts there
//     is no call to fail, and a panic crashes the program as on any goroutine:
//     the timer that stops a key once its Linger runs out, which calls the
//     stop func and the Stopped hook, and the one [Run] and [Poll] call their
//     function on, unless that function recovers it.
package streamflight
