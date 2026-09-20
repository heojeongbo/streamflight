# Changelog

## v0.3.0 — 2026-09-20

A third delivery shape, and three things every consumer was writing by hand,
taken from a real one. Purely additive: nothing to rewrite, and the only change
to the exported surface is seven new names.

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

  The key stores one value however many subscribers sample it, so the cost does
  not grow with them, and `Latest` never waits for a delivery:

  | Subscribers | `Subscribe`, read by a goroutine | `SubscribeLatest` |
  |---:|---:|---:|
  | 1 | 37 ns | 39 ns |
  | 10 | 463 ns | 47 ns |
  | 100 | 14.7 µs | 182 ns |

  `Subscription.Wait(ctx, t)` blocks until a value newer than `t` arrives, for
  reading back what you just wrote: note the time, issue the write, wait past
  it rather than sampling the value the write has not reached yet. Arrival
  times are strictly increasing within a key, so two values a caller can tell
  apart always have times it can tell apart, whatever the clock's resolution.

  A key nobody samples is unaffected: `Emit` stays at 5.4 ns and allocates
  nothing. There is no value until the first one emitted after the first
  sampler joined, and whether a value is still current is the caller's to
  decide from the arrival time `Latest` returns — silence on a topic published
  only when it changes means nothing changed, and on a sensor means the sensor
  is gone.

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
  runs before the opening subscriber is attached.

- **`Group.ReplayFor`** — `Replay` for one key, replacing it. A topic that wants
  1 and a stream of events that wants 0 can now share one Group rather than
  needing one each. It runs with no Group lock held, like `Source`.

- **`ErrPollInterval`** — why opening a key fails when `Poll` was given an
  interval that is not positive, rather than spinning.

### Fixed

- **A subscription could report no reason for ending.** `end` closed the value
  channel before `done`, so a reader woken by that close could reach `Err` and
  find nothing — and ranging over `C` and then asking `Err` is the idiom this
  README and `ExampleRun` both use. The window never lost the reason in 20000
  rounds here, but widening it with a single `runtime.Gosched` lost it in 19533
  of 20000, and none once the two closes were in the other order.

  A consumer selecting on both `Done` and `C` may now observe the end while
  values are still queued. `C` goes on yielding them.

## v0.2.0 — 2026-09-20

The Group used one lock for every key, and held it across the user's `Source`
and stop func. That made keys share a fate they were never supposed to share:
one stalled subscriber could freeze the whole Group, and opening keys that have
nothing to do with each other happened one at a time. This release gives the
"open and stop never overlap" job to the key itself, so the lock no longer
spans anyone's code.

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

- **Two fewer allocations per subscribe.** `SubscribeFunc` join and leave goes
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

- **A concurrent second `Group.Close` waits for the first** instead of returning
  immediately, so that when any `Close` returns every upstream really has been
  stopped. Sequential `Close` calls are unaffected.

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

`go get github.com/heojeongbo/streamflight@v0.2.0`. Nothing to rewrite. Three
things are worth a look, all of them pre-existing bugs this release stops
hiding:

1. Run `go test -race ./...`. If a `Source` closure or an `Opened`/`Stopped`
   hook writes shared state without a lock, it was serialized by the Group
   lock before and is not now. Logging and metrics calls are already safe; an
   unguarded `append` to a slice or write to a map is not.
2. Check any `Group.Initial` for a `send` that escapes the call. That now
   panics.
3. If you call `Group.Close` from more than one goroutine at once, the second
   call now blocks until teardown finishes.

## v0.1.0

First release.
