# Changelog

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
