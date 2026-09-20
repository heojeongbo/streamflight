# Changelog

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
