# Internal lifecycle and locking

This is a maintenance guide for `group.go`, `flight.go`, and `subscription.go`.
The public API contracts live in their Go documentation. Keep this guide in
sync when changing lifecycle transitions or moving callbacks between goroutines.

## Key ownership

`g.flights[key]` reserves a key from opening until cleanup finishes. A naturally
ended upstream still owns its slot until its stop runs. Delivery ending and
key ownership ending are separate transitions.

| `f.st` | Meaning | Transition owner |
|---|---|---|
| `opening` | Source is running; joining callers wait | `acquire` creates it; `open` publishes success or failure |
| `live` | Source succeeded; the upstream can be joined, or may already have ended | `open` publishes it; `claimLocked` takes responsibility for stopping it |
| `stopping` | One caller owns cleanup; the key cannot reopen | `claimLocked` enters it; `doStop` releases the slot after cleanup |
| `dead` | Open failed, or cleanup finished; the map entry is gone | `open` on failure, or the deferred cleanup in `doStop` |

All state transitions and map changes hold `g.mu`. `refs` counts acquired
subscription references, including callers not yet attached and ended
subscriptions their owners have not closed. `pending` counts callers waiting
for an open. `idleLocked` stops or lingers only when both counts are zero.
Group shutdown can claim a live flight regardless of these counts. A new
subscriber can also claim an ended flight once no opening waiters remain;
those waiters share the original open's result.

## Delivery and end reporting

| Field | Meaning | Synchronization and writers |
|---|---|---|
| `quit`, `byItself`, `selfErr` | Decide whether End or a stop won; record the original end error; release blocked sends | `quitOnce` runs `endByItself` or `unblock`; readers of the decision complete that Once first |
| `ended` | The upstream has ended. The Group reads it without taking `f.mu`, so a stalled delivery stalls no other key; Emit reads it under `f.mu`, refusing a value emitted after an End was decided but before `finish` ran | Atomic; set by `endByItself` or `finish`, never reset |
| `done`, `endErr` | Subscribers have been ended and Replay has been cleared | `f.mu`; written by `finish` |
| `endedOwed` | A natural end still needs its Ended hook reported | `f.mu`; set by `finish`, cleared before invoking the hook by `reportEndedLocked` |

An End can set `ended` before it obtains `f.mu`, so `ended` can be true while
`done` is false. A stop that acquires the delivery lock first must preserve
`selfErr`. After `endClosed`, `done` can be true with `endedOwed` still true:
subscriber termination has completed, but a caller still owes the hook.

`End` and `endStopped` report pending Ended hooks under `f.mu`, exactly once,
before stop and Stopped. `endClosed` runs no user hooks: Group.Close can call it
on a helper goroutine where a panic cannot propagate to the closing caller.
A sampler skips Replay delivery, but its Initial callback still runs under
`f.mu`; only Emit changes the shared latest value.

## Lock rules

| Lock | Protects | What may run while held |
|---|---|---|
| `g.mu` | Key map, ownership state, reference counts, linger timers, phase waiters | Joined and Left hooks; the short `quitOnce` operation |
| `f.mu` | Delivery lists, Replay, attachment, subscription termination, end reporting | Subscriber callbacks, Initial, Now, Dropped, Evicted, Ended |
| `latestMu` | Latest value, timestamp, availability and change notification | No user code |
| `lockersMu` | Queue for cancellable acquisition of `f.mu` | No user code |

- Do not acquire `f.mu` or wait for Source/stop while holding `g.mu`.
- `quitOnce` does not acquire another lock or invoke user code. It is safe to
  complete while holding `g.mu` when claiming a stop.
- `latestMu` and `lockersMu` are leaf locks. Delivery or the lock helper can
  acquire them while holding `f.mu`; never wait for `f.mu` while holding either.
- Source, ReplayFor, Opened, stop and Stopped run outside these locks.
- End closes `quit` before waiting for `f.mu`. A Block subscription closes its
  own `closing` channel before Close waits for `f.mu`.

## Shutdown and panic cleanup

Group.Close first claims live keys, releasing their blocked sends. It then
ends all claimed subscribers, waiting for ongoing deliveries, before running
any of their stops. This order lets a derived upstream's stop close its
subscription to another key without waiting on a Block delivery that shutdown
has not released yet. Opening or independently stopping keys are awaited and
revisited.

`doStop` owns three obligations: end/report, invoke stop, and release the key.
Its deferred fallback invokes stop if end reporting panics; key release runs
after either normal cleanup or a panic. `stopStarted` is set before invoking
stop so a panic in stop or Stopped cannot cause a second invocation.
`stopAll` finishes the remaining claimed keys on panic; `closeAll` then handles
keys still opening or stopping. Keep those responsibilities distinct.

Relevant regression tests:

- `TestEndAllLeavesHooksForTheStoppingCaller`: deterministically holds a key's
  delivery lock and verifies that a shutdown worker invokes no user hook.
- `TestEndedHookThatPanicsFailsCloseWithEverythingStopped`: a public Close
  propagates a hook panic only after all claimed upstreams have been stopped.
- `TestEndResumingAfterEndAllReportsOnlyOnce`: a resumed End and the following
  stop preserve the original error and report Ended only once.
- `TestEndedPanicWithPanickingCleanup`: a second cleanup panic neither retries
  stop nor retains the key.
