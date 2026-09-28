# Changelog

## v0.4.6 — 2026-09-28

Additional regression tests and clearer panic-handling documentation for the
shutdown fixes in v0.4.5. No runtime behavior or API changes.

### Tests

- Force the shutdown worker path without timing assumptions, and verify that
  user hooks run on the stopping caller after subscribers have ended.
- Verify that an End resuming during shutdown reports its hook exactly once,
  preserves the original upstream error, and reports before Stopped.
- Verify that a second panic in stop or Stopped does not retry cleanup or
  retain the key after an Ended hook panics.

### Documentation

- Clarify that cleanup is guaranteed when Ended panics during a stop; an End
  called directly retains the existing upstream lifecycle.
- Document Ended panics during Group.Close and on Linger timers.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.6`.

## v0.4.5 — 2026-09-28

Two ways an `Ended` hook that panics could cost more than its own call, both
introduced in v0.4.4, which is the release that gave that hook somewhere new to
be called from. Upgrade past v0.4.4 if you set `Hooks.Ended`; nothing else is
affected, and there is nothing to rewrite.

### Fixed

- **An upstream could be dropped without being stopped.** The end is reported
  where the upstream is stopped, and the stop func ran after it, so an `Ended`
  hook that panicked took the stop with it — while the deferred clean-up gave
  the key back and let go of the stop func regardless. What the `Source` had
  opened, a socket or a goroutine, was then unreachable: not the next
  subscriber's to stop, since the key was free, and not `Group.Close`'s, since
  it was gone from the Group. The stop runs whatever the ending did now, and
  the panic goes on to fail the call once it has.

- **`Group.Close` could wait forever, or the program could die.** Close ends
  every key it has claimed before it stops any, so the `Ended` hook ran there
  too. A panic on that path skipped the stops, and the retry that follows a
  failed pass then waited on keys it had claimed itself and left unstopped,
  which nothing else will ever stop. Close ends the keys whose lock it cannot
  take at once on goroutines of its own, where a panic had no call to fail and
  crashed the program. No hook runs there now: ending a key leaves the end for
  the stop that follows, which runs on Close's own goroutine, so a panic fails
  that `Close` with every key stopped, as a panicking stop func already did.

### Documentation

- `Hooks` says an `Ended` that panics during a stop still leaves its upstream
  stopped and its key given back, and `Hooks.Ended` says which calls report it:
  the `End` itself, or whatever stops the upstream once something else has
  finished it — a `Subscription.Close`, a `Group.Close` or a `Linger` timer.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.5`. Nothing to rewrite. An
`Ended` hook may now be reported from a stop where v0.4.3 and earlier only ever
reported it from `Emitter.End`; that is v0.4.4's change, not this one, and it is
what lets an end survive the stop that overtook it.

## v0.4.4 — 2026-09-28

One root, three symptoms. An `End` marked its flight over in two steps — the
close of `quit`, then the rest under the key's lock — and everything asking
whether the upstream had ended read the second step, which a stop could get to
first. The end is decided in one place now. Nothing to rewrite.

### Fixed

- **`Hooks.Ended` was lost to a stop that came after the end.** `End` reported
  it only if it was still the one to finish the flight, so a last subscriber
  leaving, a `Linger` timer or a `Group.Close` that took the key's lock in the
  window between the two steps finished the flight first and `End` then found
  nothing to do. The error went with it: `Ended` is the only thing that carries
  why an upstream died, so a caller was left unable to tell a stream that
  failed from one that was shut down. It is reported by whoever finishes the
  flight now, still under the key's lock and still before `Stopped`.

- **A stop could close subscribers with its own reason rather than the error
  the upstream ended with**, in that same window: `nil` for an ordinary stop,
  `ErrGroupClosed` for a `Close`. An upstream that ended by itself closes its
  subscribers with its own error whatever finishes it, which is also what
  `Ended` reports, as the hook has always promised.

- **An upstream that had ended by itself still lingered.** The last subscriber
  leaving in that window armed a `time.AfterFunc` for the whole of
  `Group.Linger`, against a documented promise that it would not, and held
  the stop func — a socket, a context, a goroutine to join — for that long
  after there was nothing left to come back to. One that ends *while* it
  lingers is unchanged: it is stopped when the timer runs out, as
  `Emitter.End` says.

- **A value emitted after the upstream ended was delivered.** `Emit` dropped on
  the second step, so a value emitted in the window reached subscribers and
  entered the `Replay` ring, against the guarantee that values emitted after an
  upstream has ended are dropped. `Emit` and `End` after the first `End` now do
  nothing, as `Emitter.End` says. Of two `End`s the first decides: the second
  no longer replaces its error.

### Documentation

- `Hooks.Ended` says an `End` decides where it ends the upstream, so a stop
  that closes the subscribers first reports it rather than swallow it.
- `ErrGroupClosed` says it is not why an upstream that had already ended by
  itself closed its subscribers.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.4`. Nothing to rewrite. Worth
a look:

1. `Hooks.Ended` fires in races where it used to be silent, so a counter of
   upstreams that died by themselves will report ends it was missing. It is
   still never called for an upstream the Group had begun to stop.
2. Subscribers of an upstream that ended by itself now see that upstream's
   error even when a `Group.Close` is what closed them, where they used to see
   `ErrGroupClosed`. Code that treats `ErrGroupClosed` as "shutting down, do
   not reconnect" should treat the upstream's own error the same way when it
   arrives during a shutdown, which `Subscription.Err` has always been able to
   return.
3. Of two concurrent `End`s the first one's error is the one reported and the
   one subscribers are closed with; it used to be whichever reached the key's
   lock first.

## v0.4.3 — 2026-09-28

Keys a map cannot hold on to, which the compiler checking that `K` is
`comparable` does not rule out: one wedged `Group.Close` forever and lost the
sharing the package exists for, the other left the whole Group locked. And the
last of an `Ended` hook that reported a key the Group had begun to stop.
Nothing to rewrite, but two kinds of key that used to be accepted now panic;
see *Upgrading*.

### Fixed

- **A key not equal to itself was stored under a key nothing could find
  again.** A NaN, or any key holding one — a struct with a float field, an
  array, a `Group[any, T]` given one — went into the map under a key no lookup
  ever matched, so three things went wrong at once and none of them said so.
  Every subscriber of that key opened an upstream of its own instead of joining
  one, which is the whole of what the package promises. The `delete` that frees
  a key removed nothing, so each upstream left its entry behind for the life of
  the Group. And `Group.Close`, finding a flight it could neither claim nor be
  woken by, waited forever, as did every `Close` after it: one subscribe whose
  open merely *failed* under such a key was enough to make the Group
  unclosable. Subscribing with one now panics where the key is passed.

- **A key with no equality at all left the whole Group locked.** A slice, map
  or func in a `Group[any, T]` panicked where the map hashed it, which is under
  the lock the whole Group shares, and nothing ever let that lock go:
  subscribing to any other key, closing a subscription already open, closing
  the Group and the `Linger` timer all waited on it forever, no upstream could
  be stopped, and `Emit` and `End`, which take only the key's own lock, went on
  working, so the Group looked alive while its whole lifecycle was wedged. The
  first `Subscribe` on a brand-new Group was enough, since Go checks the key
  before the empty-map shortcut. Such a key is now compared before the Group
  takes its lock, so the panic reaches the caller and leaves the Group as it
  found it.

- **`Hooks.Ended` could still report a key `Group.Close` had begun to stop.**
  v0.4.2 decided that by the close of the flight's `quit`, which is the first
  thing a stop does. But a stop begins before that, when it claims the key
  under the Group's lock, and an `End` arriving between the claim and the close
  was still counted as an end of the key's own. `Group.Close` claims every live
  key in one critical section and only then releases them one by one, so the
  window is as wide as the number of keys: an upstream that ended by itself
  during a graceful shutdown, a dropped connection or a `Run` function
  returning, could be reported as having died on its own. The claim now closes
  `quit` itself, so the flight's own `sync.Once` is the single place that
  decides. The regression test for v0.4.2 took the close of `quit` for the
  first step of a stop, which is why it never covered this.

  A delivery waiting on a `Block` subscriber of a key being stopped is now
  released as the key is claimed rather than as its stop runs, which is a
  little earlier on the `Close` path.

### Documentation

- What a key must be beyond `comparable`, on `Group` and among the rules in the
  package documentation: equal to itself, and having equality at all.
- The Group's lock is a leaf: a flight's `quitOnce` is the one lock taken under
  it, and what runs under that closes a channel and takes no lock.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.3`. Nothing to rewrite. Worth
a look:

1. Two kinds of key now panic where they are subscribed, rather than be
   accepted and then wedge the Group. One not equal to itself panics with
   `streamflight: key NaN is not equal to itself, so its upstream could be
   neither shared nor forgotten`; one whose dynamic type has no equality panics
   with Go's own `comparing uncomparable type`, from the comparison that now
   happens before the lock. A `Group[float64, T]` keyed by a computed ratio, or
   a `Group[any, T]` keyed by whatever a request carried, is worth a check
   where the key is made.
2. `Hooks.Ended` no longer fires for a key `Group.Close` claimed first, even
   when the `End` lands in the first instants of the stop. A counter that used
   to see upstreams die by themselves during shutdown now sees only `Stopped`
   for them.

## v0.4.2 — 2026-09-28

A relay that returned its client's write error about half the times the client
left, callers that each reopened an upstream that failed as it opened, and what
further reviews found: misuses that were accepted quietly, and documentation
that promised more or less than the code does. Nothing to rewrite, but a few
calls that used to be accepted now panic; see *Upgrading*.

### Fixed

- **`Drain` wrote to a client that had gone.** With values queued as its
  context ended, `select` picked between the two at random, so about half the
  time `Drain` called `send` once more and returned the error of that write
  rather than nil: a handler that returns `sub.Drain(stream.Context(),
  stream.Send)` reported an ordinary disconnect as a failure. `Drain` now
  looks at the context before it takes each value and leaves what is queued
  for the next `Drain`, and takes an error from a `send` in progress as the
  context ended for the client going away too.

- **Callers waiting on an open that ended as it opened each opened the key
  again.** A `Run` or `Poll` function that fails at once, as on a refused
  connection, ends the upstream before its `Source` returns. Every waiter then
  found it ended, stopped it and opened another, one after the other: eight
  waiters, eight opens. They now share how the open went, as they already
  shared an error the `Source` returned, each with a subscription ended with
  that error, and so does a subscriber that arrives before they have joined.

- **A Context subscribe that opened a key and then gave up stopped it under
  those waiting on it.** With `Linger` at zero, giving its reference back
  stopped the upstream before the callers waiting on the same open had
  joined, and they opened it again. An upstream is no longer stopped while
  callers that waited for it to open have yet to join.

- **`Wait` could miss a value that arrived as the subscription ended**,
  returning `ok == false` while `Latest` held a newer value.

- **A second `Group.Close` returned nil** where the first returned the errors
  of the stop funcs, so which caller saw a failed stop depended on which got
  there first. Every call now returns the same error, as `Subscription.Close`
  does, and a stop func or `Stopped` hook that panics in the first no longer
  loses the errors of the stops that returned.

- **A subscription still held kept a stopped upstream's resources alive.** The
  stop func, and whatever it captured, such as a connection or a buffer,
  stayed reachable from any subscription still referenced, a sampler kept to
  read `Latest` after the end among them. It is let go once called.

- **`Hooks.Ended` could report a key `Group.Close` had begun to stop**, when
  that key ended in reply to another key being closed. It now reports only an
  end that came before any stop.

- **Misuses that were accepted quietly now panic where they are made**, as
  `SubscribeFunc(key, nil)` has since v0.4.1: `Run(nil)` and `Poll(d, nil)`,
  which crashed the program later from the goroutine they start; a nil
  `SubscribeOption`, which was a bare nil dereference; `Drain` with a nil
  `send`, which lost the value it had taken; `Wait` with a nil context; and
  `Drain` on a `Subscription` no Group made, which blocked.

### Performance

- **An `Emit` costs the same however many samplers a key has.** Samplers are
  kept apart from the subscribers values are delivered to, rather than
  stepped through one by one under the key's lock:

  | Samplers | before | after |
  |---:|---:|---:|
  | 1 | 43 ns | 43 ns |
  | 10 | 49 ns | 43 ns |
  | 100 | 175 ns | 43 ns |

- Callers queued for a held key leave the queue in constant time, and closing
  every subscriber one `Emit` evicted is linear rather than quadratic.

- Joining and leaving a key is 2–6% slower, about 4 ns, for counting the
  callers waiting on an open and keeping samplers apart.

### Documentation

- `Joined` and `Left` count one upstream's subscribers, not the key's: for a
  gauge per key that outlives an upstream's end, count their calls. Which
  hooks are never called at once is spelled out, and `Ended` can come before
  `Opened`.
- Closing a subscription waits for a delivery a `Block` subscriber holds up,
  and so does a derived key's stop. Groups that feed one another must be
  closed together, and a delivery must not wait on a stop in the same Group.
- A Context subscribe that gives up can be the one that stops what it leaves
  with nobody, and runs the stop func before it returns.
- What a `Run` function returns once the Group is stopping its key is
  discarded; `Initial`'s `send` is for one goroutine at a time; values
  emitted on other goroutines can reach a `SubscribeFunc` function before it
  returns.
- The README's performance tables are measured again, on an Apple M3 Max.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.2`. Nothing to rewrite. Worth
a look:

1. Calls given a bad argument now panic where the mistake is made. `Run(nil)`
   and `Poll(d, nil)` panic when called, not when a key opens: a Source built
   from a nil function panics even if it is never opened. A nil
   `SubscribeOption` panics with a message. `Drain` panics on a nil `send`
   even with nothing to send, and `Wait` on a nil context even with a value
   there to return. `Drain` on a nil context always panicked; only the
   message is new. `Drain` on a `Subscription` no Group made panics again, as
   in v0.4.0.
2. Callers that were waiting on an open when its upstream ended, and any that
   arrive before they have joined it, get a subscription already ended with
   its error rather than each a fresh upstream, as the caller that opened it
   always did. To try again, check `Err` or `Done` after subscribing and
   subscribe again.
3. `Group.Close` returns the errors of the stop funcs on every call, not only
   the first. Code that reports the error of each of two Closes reports a
   failed stop twice.
4. `Drain` returns nil, not the error of `send`, when its context ended while
   `send` was failing.

## v0.4.1 — 2026-09-27

A hang in `Group.Close`, two silent misuses turned into loud ones, and what a
review of real use asked for: a subscribe that a cancelled request can walk
away from, and hooks that tell a failing upstream from a stopped one and a
slow consumer from a refused value. Nothing to rewrite, but two calls that
used to be accepted now panic; see *Upgrading*.

### Added

- **`SubscribeContext`, `SubscribeFuncContext` and `SubscribeLatestContext`**
  — `Subscribe` and its siblings waited as long as it took: for another
  goroutine opening or stopping the key, and for a delivery holding the
  key's lock, which a `Block` subscriber can make last indefinitely. The
  Context variants give up, returning the context's error, while they wait on
  others. As with `net.DialContext`, the context bounds joining only: it has
  no effect on a subscription once returned, and giving up never ends the
  upstream for anyone else.

  They cannot interrupt code that takes no context, such as the `Source` of
  the key they are opening, so they let it return and then give back what
  they took, the way a last subscriber leaving would. Callers waiting for a
  held key's lock queue for it, and one goroutine per key waits on their
  behalf, so callers that keep giving up on a stalled key cost it one
  goroutine, not one each. On a key whose lock is free, a Context subscribe
  costs what `Subscribe` does.

- **`Hooks.Ended` and `Hooks.Evicted`** — `Stopped` reports the error of a
  stop func, not why an upstream ended by itself, and `Dropped` reports
  values a full queue lost, not subscribers `Evict` cut off. `Ended` is called
  with the error an upstream ended with, always before `Stopped` reports the
  same upstream; `Evicted` for each subscriber `Evict` cuts off, live or
  catching up.

### Fixed

- **`Group.Close` could hang on a derived key.** A derived key's stop closes
  its subscription to the key it is fed from, and so waits for that key's
  delivery. If a `Block` subscriber there had stopped reading, only stopping
  that key would have released it, and `Close` never got that far whenever it
  came to the derived key first: 87 hangs in 100 in a reproduction. `Close`
  now releases the waiting deliveries of every key it is about to stop, then
  ends the subscribers of each key, one key not waiting on another's
  delivery, then runs the stops one at a time. A `Block` subscriber released
  this way is ended as soon as its own key's delivery lets go, never left
  live and refused values for as long as other keys take.

- **`SubscribeFunc(key, nil)` quietly made a sampler, and an unknown
  `Overflow` quietly acted as `DropOldest`.** Both now panic where the mistake
  is made, as a nil `Source` already does.

### Performance

- **Evicting many subscribers in one `Emit` is linear.** Each eviction used
  to move every subscriber after it. One `Emit` evicting every subscriber:

  | Subscribers | before | after |
  |---:|---:|---:|
  | 100 | 4.3 µs | 2.3 µs |
  | 1000 | 76 µs | 22 µs |
  | 10000 | 4.85 ms | 207 µs |

- `Emit` to function subscribers is 3–12% faster. Joining and leaving a key
  is 4–11 ns slower (5–9%), for the plumbing a Context subscribe needs.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.1`. Nothing to rewrite. Two
things are worth a look:

1. `SubscribeFunc` with a nil function, and `WithOverflow` with a value that
   is not one of the four policies, now panic. The first used to make a
   subscription that sampled instead of being called; the second used to act
   as `DropOldest`.
2. `Hooks.Joined` can now be followed by `Left` for a subscriber that never
   got a subscription: one whose Context subscribe gave up waiting for the
   key's lock. The counts stay balanced.

## v0.4.0 — 2026-09-26

No new API. Two places where the package lost values without saying so, and
a hook or clock that panicked could wedge far more than the call it ran in.
The documentation now starts from what is done with each value, and says what
the code does where it did not.

### Changed

- **A sampler that opens a key keeps what its Source emits while opening.** A
  key used to keep its newest value only from the moment its first sampler
  was attached, which is after the Source returns. The current state a Source
  sends as it subscribes, and `Poll`'s first tick whenever that landed before
  the attach, never reached a `SubscribeLatest` subscriber that opened the
  key, and `Replay` could not help, since a sampler is sent nothing by it. A
  key opened by a channel or function subscriber is unchanged.

- **An `Evict` subscriber whose catch-up does not fit is evicted.** What
  `Replay` and `Initial` send a joining subscriber went through `DropOldest`
  whatever its policy, so an `Evict` subscriber with a queue shorter than a
  snapshot lost the start of it and carried on from a corrupt base. It now
  ends with `ErrEvicted`, as it would for a live value it had no room for.
  `Subscribe` returns it already ended, the same as a subscription to an
  upstream that has ended. Other policies are unchanged.

- **The first arrival time is always after the zero time.** Waiting past the
  zero time now finds the value there even under a `Group.Now` that reports
  the zero time, such as a fake clock nobody has advanced.

### Fixed

- **A `Joined` or `Left` hook that panicked wedged the whole Group.** Both run
  under the Group lock and were called with nothing to release it, so
  `Subscribe`, `Close` and `Group.Close` on every key stopped returning. A
  `Joined` that panicked as a key finished opening also left whoever waited
  on that key asleep for good. A panic in the caller's code now fails the call
  it ran in: the lock is released, waiters are woken, and what the Source
  opened stays stoppable, including when `Opened` panics after it. A
  subscription whose `Initial` or catch-up panics gives its reference back.

- **`Group.Close` could leave an upstream running if a stop panicked.** A stop
  func or `Stopped` hook that panicked ended `Close` there, so upstreams it had
  claimed but not reached, and one another goroutine was opening, were never
  stopped, while every later `Close` returned at once. `Close` now stops them
  all before its panic goes on.

- **A `Group.Now` that panicked locked a key's samplers out.** It was read
  under the lock `Latest` and `Wait` take, with nothing to release it.

### Documentation

- The README starts from what is done with each value: a write that can block
  is `Subscribe` and `Drain`, short work is `SubscribeFunc`, the current value
  is `SubscribeLatest`. Its first example used to write to a connection from
  `SubscribeFunc`, the one thing the rest of it says not to do.
- `Wait` takes its time from `Latest`, which is on the clock of `Group.Now`,
  rather than from `time.Now`, and says whether to take it before or after the
  write. New examples: `Group.SubscribeLatest`, `Subscription.Wait`, and a
  `Poll` whose interval comes from the key.
- One list of what a delivery may and may not call, a single-key Group
  described as the reference count it is, and a panic rule that says which
  goroutines have no call to fail.
- Sentences that did not match the code, among them `Poll`'s first tick, which
  can run before the opener is attached rather than always does. The v0.3.0
  entry below is corrected the same way where it gave the same advice.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.4.0`. Nothing to rewrite. Two
things are worth a look:

1. A subscription made with `WithOverflow(Evict)` on a Group with `Replay` or
   `Initial` now comes back already ended with `ErrEvicted` when its buffer is
   shorter than what it is caught up on. Size the buffer for the snapshot, or
   check `Err` after `Subscribe`.
2. A `SubscribeLatest` subscriber that opens a key whose Source emits while
   opening now finds that value in `Latest` at once. Code that took `ok ==
   false` straight after subscribing to mean "nothing yet" sees it.

## v0.3.0 — 2026-09-22

A third delivery shape, and three things every consumer was writing by hand,
taken from a real one. Purely additive: nothing to rewrite, and the only change
to the exported surface is eight new names.

### Added

- **`Group.SubscribeLatest(key)` and `Subscription.Latest()`** — a third
  delivery shape, for a subscriber on its own clock that wants the current
  value whenever it looks rather than every value as it arrives. Nothing is
  queued and nothing is called: the key keeps its newest value and the
  subscriber samples it.

  A channel cannot do this, because receiving consumes — a reader that looks
  while the upstream is quiet finds an empty queue rather than the value that
  is still true. `Replay: 1` was already the push half of the same idea; this
  is the pull half.

  The key stores one value however many subscribers sample it, so a subscriber
  costs a step through a loop rather than a channel send, and `Latest` never
  waits for a delivery:

  | Subscribers | `Subscribe`, read by a goroutine | `SubscribeLatest` |
  |---:|---:|---:|
  | 1 | 37 ns | 39 ns |
  | 10 | 463 ns | 47 ns |
  | 100 | 14.7 µs | 182 ns |

  `Subscription.Wait(ctx, t)` blocks until a value newer than `t` arrives, for
  reading back what you just wrote: take an arrival time from `Latest` rather
  than `time.Now`, issue the write, and wait past it rather than sampling the
  value the write has not reached yet (`Wait` says whether to take the time
  before or after the write). Arrival times are strictly increasing within a
  key, so two values a caller can tell apart always have times it can tell
  apart, whatever the clock's resolution.

  A key that has never had a sampler is unaffected: `Emit` stays at 5.4 ns and
  allocates nothing. There is no value until the first one emitted after the
  first sampler joined, and whether a value is still current is the caller's
  to decide from the arrival time `Latest` returns — silence on a topic
  published only when it changes means nothing changed, and on a sensor means
  the sensor is gone.

- **`Subscription.Drain(ctx, send)`** — the body of a handler relaying one key
  to one client: the select on the context and the channel, the closed-channel
  case, the send error. `send` is a `func(T) error`, so it is whatever the
  protocol's write is — gRPC's `Send` fits as a method value, everything else
  in a two-line closure — and this package stays ignorant of all of them. A
  write that can block belongs there rather than in `SubscribeFunc`, where it
  would hold up every other subscriber of the key.

  ```go
  sub, err := g.Subscribe(key, streamflight.WithBuffer(16))
  if err != nil {
      return err
  }
  defer sub.Close()
  return sub.Drain(stream.Context(), stream.Send)
  ```

- **`Poll(interval, tick)`** — a `Source` from a function called on an
  interval, for an upstream that is a repeated request rather than a
  subscription. It fires on open, so a subscriber sees something immediately
  instead of after one interval, and reschedules an interval after the previous
  call *returned*. Not a `time.Ticker`: a Ticker keeps the tick a slow call
  missed and fires again at once, so a call that outruns its interval runs back
  to back with no idle at all. Pair it with `Replay: 1`, since the first tick
  can run before the opening subscriber is attached.

- **`Group.ReplayFor`** — `Replay` for one key, replacing it. A topic that wants
  1 and a stream of events that wants 0 can now share one Group rather than
  needing one each. It runs with no Group lock held, like `Source`.

- **`Group.Now`** — where a value gets the arrival time `Latest` reports and
  `Wait` waits past; defaults to `time.Now`. Set it to age a value from a test,
  because whether a value is still current is the caller's to decide and
  deciding it is worth a test. A key that has never had a sampler never calls
  it.

- **`ErrPollInterval`** — why opening a key fails when `Poll` was given an
  interval that is not positive, rather than spinning.

- A guarantee: **once a key has a sampler, a value emitted to it becomes the
  key's newest before it is delivered to any subscriber.** A subscriber woken
  by such a value therefore reads that same value from `Latest`, never the one
  before it — which is what lets one subscription carry the state and another
  the edge, the shape a latch with a change hook needs. It holds for values
  delivered as they are emitted, not for what Replay sends a subscriber as it
  joins. The implementation already did this; now it is promised.

### Fixed

- **A subscription could report no reason for ending.** `end` closed the value
  channel before `done`, so a reader woken by that close could reach `Err` and
  find nothing — and ranging over `C` and then asking `Err` is the idiom this
  README and `ExampleRun` both use. The window never lost the reason in 20000
  rounds here, but widening it with a single `runtime.Gosched` lost it in 19533
  of 20000, and none once the two closes were in the other order.

  A consumer selecting on both `Done` and `C` can now see `Done` before `C` is
  closed. Values may still be queued, and `C` goes on yielding them.

## v0.2.0 — 2026-09-20

The Group used one lock for every key, and held it across the user's `Source`
and stop func. That made keys share a fate they were never supposed to share:
one stalled subscriber could freeze the whole Group, and opening keys that have
nothing to do with each other happened one at a time. This release gives the
"open and stop never overlap" job to the key itself, so the lock no longer
spans a `Source` or a stop func.

No call site changes. The exported API is unchanged apart from
`SubscribeOption`, whose parameter type is unexported and so could never be
written outside the package.

### Fixed

- **A subscriber that stopped reading froze the whole Group.** Checking whether
  an upstream had ended took the key's delivery lock while holding the Group's
  lock. A `Block` subscriber holds the delivery lock for as long as it is full —
  that is what `Block` is for — so one stalled subscriber plus one concurrent
  `Subscribe` on the same key blocked `Subscribe` and `Close` for *every* key.
  `Group.Close` could not return either, even though closing the Group is
  documented as one of the three things that release a `Block` delivery.

- **A Source built on another key of the same Group deadlocked.** Its stop func
  ran under the Group lock, so a stop that waits for a goroutine — which is what
  `Run`'s stop does — deadlocked against that goroutine releasing its own
  subscription. Deriving one stream from another is now supported rather than
  merely undocumented.

### Performance

- **Keys open independently.** Opening keys that share nothing no longer
  serializes. With a `Source` that takes 1 ms, roughly what subscribing to a
  broker costs:

  | Keys opened at once | 1 | 8 | 32 |
  |---|---:|---:|---:|
  | before | 1.20 ms | 9.17 ms | 36.40 ms |
  | after | 1.19 ms | 1.28 ms | 1.24 ms |

- **Fewer allocations per subscribe.** `SubscribeFunc` join and leave goes
  from 3 allocations and 352 B to 2 and 240 B; `Subscribe` from 5 and 496 B to
  3 and 368 B.

### Changed

These do not require a code change, but they can affect code that was already
relying on something the package did not promise. See *Upgrading*.

- **`Source`, `Hooks.Opened` and `Hooks.Stopped` now run with no Group lock
  held, and concurrently for different keys.** Previously the Group lock
  serialized them all. Anything they touch now needs its own synchronization.
  `Joined` and `Left` still run under the Group lock, so their counts stay in
  order; keep those two short.

- **Retaining the `send` given to `Group.Initial` and calling it later now
  panics** with a message naming the rule. It was already documented as valid
  only during the call, and doing it raced the key — but it could appear to
  work. It now fails every time instead of sometimes.

- **Concurrent subscribers to a key whose open fails now share one attempt** and
  its error, rather than each calling `Source` again. This is what "one upstream
  per key" already promised for the success path.

- **Subscribing to a Group with no `Source` panics** with a message instead of a
  bare nil pointer dereference.

### Added

- A guarantee: **keys are independent**. Opening or stopping one key never
  blocks another, and neither does a subscriber that has fallen behind.
- `scripts/test.sh` checks formatting and runs the race tests five times over;
  `STREAMFLIGHT_TEST_COUNT` raises that. CI runs on `linux/amd64` and
  `linux/arm64`.

### Upgrading

`go get github.com/heojeongbo/streamflight@v0.2.0`. Nothing to rewrite. Two
things are worth a look, both of them pre-existing bugs this release stops
hiding:

1. Run `go test -race ./...`. If a `Source` closure or an `Opened`/`Stopped`
   hook writes shared state without a lock, it was serialized by the Group
   lock before and is not now. Logging and metrics calls are already safe; an
   unguarded `append` to a slice or write to a map is not.
2. Check any `Group.Initial` for a `send` that escapes the call. That now
   panics.

## v0.1.0 — 2026-09-19

First release. `streamflight` is `singleflight` for streams: it shares one
upstream per key among any number of subscribers. The first subscriber of a key
opens the upstream, later subscribers join it, and the last one to leave stops
it.

### Added

- **`Group`, `Source` and `Emitter`** — a `Group` opens a key by calling its
  `Source`, which returns the func that stops the upstream. The `Source`
  delivers values through an `Emitter`, whose `Emit` is safe to call from
  several goroutines. `Group.Close` stops every upstream and ends every
  subscription with `ErrGroupClosed`.

- **`Emitter.End`** — for an upstream that ends by itself. Every subscriber is
  closed with the given error, or `io.EOF` if it is nil, and the next
  subscriber of the key opens a fresh upstream.

- **`Group.SubscribeFunc` and `Group.Subscribe`** — two ways to receive, each
  returning a `Subscription` whose `Close` leaves the key. `SubscribeFunc`
  calls a function for each value, a live one on the goroutine that emitted
  it: nothing is queued, but a slow function holds up every other subscriber
  of the key. `Subscribe` queues values on the channel `Subscription.C`
  instead, which holds one value unless `WithBuffer` asks for more. Once
  `Subscription.Done` is closed, `Subscription.Err` says why the subscription
  ended.

- **`Overflow` policies, chosen with `WithOverflow`** — for a channel
  subscriber that falls behind. `DropOldest`, the default, discards the oldest
  queued value, for state where only the latest matters. `DropNewest` refuses
  the arriving value, keeping the start of a burst. `Block` waits for the
  subscriber, and so do the `Source` and every other subscriber of the key.
  `Evict` closes the subscriber with `ErrEvicted`, for deltas, where a gap
  corrupts everything after it. `Subscription.Dropped` counts the values a
  full queue discarded.

- **`Group.Replay` and `Group.Initial`** — what a subscriber is sent when it
  joins, before any live value. `Replay` is how many of the latest values a
  key remembers and sends, for state that is published only when it changes;
  a channel subscriber keeps the newest that fit its buffer. `Initial` sends
  it values no other subscriber receives, such as a snapshot for a stream of
  deltas.

- **`Group.Linger`** — how long an upstream keeps running after its last
  subscriber leaves, so a subscriber that comes back in time, such as a
  reloaded page, reuses it instead of opening it again.

- **`Group.Hooks`** — reports opens, stops, joins, leaves and drops, for
  logging and metrics.

- **`Run`** — a `Source` made from a function that produces values until its
  context is done, such as a poller. Stopping the key cancels the context and
  waits for the function. If the function returns while the context is still
  live, the upstream ends with its error, or `io.EOF` if that is nil.

- **`ErrClosed`, `ErrEvicted` and `ErrGroupClosed`** — why a subscription
  ended, besides the upstream's own error. Subscribing to a closed `Group`
  fails with `ErrGroupClosed`.

### Guarantees

- **One upstream per key.** Concurrent subscribers of a key open it once, and
  it is stopped once.
- **Open and stop are serialized.** A key is never re-opened before its
  previous upstream has been stopped.
- **In order, one at a time.** Values reach every subscriber in the order they
  were emitted, and a subscriber is never delivered to concurrently, even when
  the `Source` emits from several goroutines.
- **Nothing after Close.** Once `Subscription.Close` returns, its subscriber
  is never delivered to again.

### Rules

- Every subscriber of a key receives the same value. Treat it as read-only.
- `Source`, its stop func and every hook except `Dropped` run under one lock
  shared by the whole Group. Keep them short.
- A `SubscribeFunc` function, `Initial` and `Hooks` must not call back into
  the Group, and no subscription may be closed from inside a `SubscribeFunc`
  function: both deadlock.
- Always close a `Subscription`, even one that has already ended. An upstream
  is stopped only when all of its subscriptions are closed.

### Performance

On an Apple M3 Max with Go 1.26.4, a 4 KiB message decoded by copying and
checksumming it. Dedicated decodes it once per subscriber, as an upstream per
subscriber would; shared decodes it once and `Emit`s it to `SubscribeFunc`
subscribers. Time per message:

| Subscribers | Dedicated | Shared | Speed-up |
|---:|---:|---:|---:|
| 1 | 745 ns | 766 ns | 1× |
| 10 | 7.6 µs | 800 ns | 9.5× |
| 100 | 77 µs | 990 ns | 78× |

`Emit` itself allocates nothing. A channel costs most when its reader is
parked, since each send wakes a goroutine: one `Emit` to 100 `Subscribe`
subscribers, each read by its own goroutine, takes 12.3 µs, against 224 ns to
100 `SubscribeFunc` subscribers.

### Installing

`go get github.com/heojeongbo/streamflight@v0.1.0`. Requires Go 1.26.
