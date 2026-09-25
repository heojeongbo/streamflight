# streamflight

`singleflight` for streams: share one upstream per key among any number of
subscribers.

```sh
go get github.com/heojeongbo/streamflight
```

Opening an upstream per consumer is easy and gets expensive. Three browser tabs
watching the same topic become three broker subscriptions, three decoders and
three copies of the same bytes. `streamflight` keys the upstream instead. The
first subscriber opens it, later subscribers join it, and the last one to leave
stops it.

| | `singleflight` | `streamflight` |
|---|---|---|
| Collapses | concurrent **calls** for a key | concurrent **subscriptions** for a key |
| Runs | the function once | the upstream once |
| Delivers | one value | a stream of values |
| Ends | when the function returns | when the last subscriber leaves, or the upstream ends |
| A late arrival | shares the call if still in flight | joins the running stream, optionally sent what it missed |

## Usage

A `Group` needs a `Source`: how to open the upstream of a key.

```go
g := &streamflight.Group[string, []byte]{
	// Called once per upstream, by the first subscriber of the key.
	Source: func(topic string, e streamflight.Emitter[[]byte]) (stop func() error, err error) {
		sub, err := broker.Subscribe(topic, func(msg []byte) { e.Emit(decode(msg)) })
		if err != nil {
			return nil, err
		}
		return sub.Close, nil
	},
}
defer g.Close()
```

Then subscribe. Which way depends on what is done with each value:

| Each value is | Subscribe with | Because |
|---|---|---|
| sent somewhere that can block, such as a client | `Subscribe` and `Drain` | each subscriber has its own queue, so a slow one holds up nobody else unless its `Overflow` is `Block` |
| handled by short work that never blocks | `SubscribeFunc` | it is called on the emitting goroutine, with nothing queued |
| read as the current value, on the reader's own clock | `SubscribeLatest` and `Latest` | the key keeps its newest value once, and reading it consumes nothing |

### Relaying to a client

A handler that relays one key to one client is `Subscribe` and `Drain`:

```go
func (s *Server) Watch(req *Request, stream grpc.ServerStreamingServer[Status]) error {
	sub, err := s.group.Subscribe(req.Key, streamflight.WithBuffer(16))
	if err != nil {
		return err
	}
	defer sub.Close() // the last Close stops the upstream
	return sub.Drain(stream.Context(), stream.Send)
}
```

`send` is a `func(T) error`, so it is whatever the protocol's write is — no
interface to satisfy and nothing for this package to know about:

| Sink | `send` |
|---|---|
| gRPC server stream | `stream.Send` |
| WebRTC data channel | `func(b []byte) error { return dc.Send(b) }` |
| WebRTC media track | `func(b []byte) error { _, err := track.Write(b); return err }` |
| WebSocket | `func(b []byte) error { return c.WriteMessage(websocket.BinaryMessage, b) }` |
| SSE | `func(v Event) error { if err := enc.Encode(v); err != nil { return err }; f.Flush(); return nil }` |

`send` runs on the caller's goroutine, so it may block: no other subscriber of
the key waits for it, and this subscription's `Overflow` policy decides what
falling behind costs. That is why a network write belongs here and not in
`SubscribeFunc`.

The channel is also there to read directly. It is closed when the subscription
ends, and `Err` says why:

```go
for b := range sub.C {
	render(b)
}
if err := sub.Err(); !errors.Is(err, io.EOF) {
	return err // ErrClosed, ErrEvicted, ErrGroupClosed, or the upstream's own error
}
return nil // the upstream ended by itself
```

### Short work

Work that is short and never blocks is `SubscribeFunc`, which queues nothing:

```go
sub, err := g.SubscribeFunc("prices/BTC", func(b []byte) {
	bytesIn.Add(int64(len(b))) // every other subscriber of the key waits for this
})
if err != nil {
	return err
}
defer sub.Close()
```

### Reading the current value

A reader on its own clock — a handler on an interval, a frame loop, a health
check — wants the value that is true when it looks, not every value as it
arrives. That is `SubscribeLatest`. A channel cannot do it, because receiving
consumes: a reader that looks while the upstream is quiet finds an empty queue
rather than the value that is still true.

```go
thermostat := &streamflight.Group[struct{}, float64]{Source: readSetpoint} // one thermostat, so one key
defer thermostat.Close()

sub, err := thermostat.SubscribeLatest(struct{}{})
if err != nil {
	return err
}
defer sub.Close()

v, at, ok := sub.Latest() // as often as you like; whether at is too old is yours to decide
```

`Wait` is for reading back what you just wrote, where the value that is there
is the one the write has not reached yet:

```go
_, seen, _ := sub.Latest()
setSetpoint(22)                 // the write
v, _, ok := sub.Wait(ctx, seen) // the newest value, once one has arrived after seen
```

Take `seen` from `Latest` rather than from `time.Now`: it is an arrival time
on the clock of `Group.Now`. On a key published only when it changes, take it
before the write, as here, since the change can arrive before the write
returns. On a key published periodically, take it after, so that what arrived
before the write returned does not count. Either way, a newer value need not
show the write, since one sampled before it can still arrive after it: when
the write shows in the value itself, wait until it does.
`ExampleSubscription_Wait` has the loop.

### Writing a Source

A Source whose upstream is a repeated request rather than a subscription is
written with `Poll`:

```go
Replay: 1, // the first tick can run before the opening subscriber is attached
Source: streamflight.Poll(3*time.Second,
	func(ctx context.Context, host string, e streamflight.Emitter[Status]) error {
		e.Emit(poll(ctx, host))
		return nil
	}),
```

The first call is on open, so a subscriber sees something immediately, and the
next is an interval after the last one *returned* — not a `time.Ticker`, which
keeps the tick a slow call missed and fires again at once, leaving a poll that
outruns its interval running back to back with no idle at all. A subscriber
from `SubscribeLatest` that opens the key needs no `Replay`: a key it opens
keeps what its Source emits while opening.

A Source that runs its own loop is written with `Run`. Setup that can fail goes
before it, in the Source, so that it fails `Subscribe` instead of ending a
stream that has just been handed out:

```go
Source: func(host string, e streamflight.Emitter[Status]) (func() error, error) {
	conn, err := dial(host)
	if err != nil {
		return nil, err // Subscribe fails with it
	}
	return streamflight.Run(func(ctx context.Context, host string, e streamflight.Emitter[Status]) error {
		context.AfterFunc(ctx, func() { conn.Close() }) // unblocks Read once the key stops
		for {
			v, err := conn.Read()
			if err != nil {
				return err // the upstream ends; subscribers see this error
			}
			e.Emit(v)
		}
	})(host, e)
},
```

Stopping the key cancels `ctx` and waits for the loop to return, so a read that
takes no context has to be unblocked some other way, as `AfterFunc` does here.
The same shape gives `Poll` an interval that depends on the key.

## Choosing the behaviour

**Falling behind.** `SubscribeFunc` queues nothing, but a slow function holds
up every other subscriber of the key. `Subscribe` queues instead, and its
`Overflow` policy decides what happens to a subscriber that falls behind:

| Policy | A full queue… | For |
|---|---|---|
| `DropOldest` (default) | discards its oldest value | state, where only the latest matters |
| `DropNewest` | refuses the arriving value | keeping the start of a burst |
| `Block` | waits for the subscriber, and so do the `Source` and every other subscriber of the key | lossless delivery to consumers that keep up |
| `Evict` | closes the subscriber with `ErrEvicted` | deltas, where a gap corrupts everything after it |

With no options the queue holds one value and drops its oldest, which suits
state and not events.

**Late subscribers.** `Group.Replay` sends a joining subscriber the latest
values, for state that is only published when it changes — or `Group.ReplayFor`
when only some keys are state, so that a topic wanting 1 and an event stream
wanting 0 can share one Group. `Group.Initial` sends it anything else first,
such as a snapshot for a stream of deltas. Neither waits for a subscriber still
inside `Subscribe`: a queue too short for what it is sent keeps the newest,
except under `Evict`, which ends the subscription with `ErrEvicted` rather than
let it start from a snapshot with a gap in it. A sampler keeps neither:
`Initial` is still called for it, but it reads what the key keeps.

**Churn.** `Group.Linger` keeps an upstream running for a while after its last
subscriber leaves, so a reloaded page reuses it instead of opening it again.

**Upstreams that end.** A Source calls `Emitter.End` when its upstream ends by
itself. Every subscriber is closed with that error, and the next subscriber
opens a fresh upstream.

**One key.** A Group need not have many. One upstream shared by whoever wants
it, such as a socket or a device, is a `Group[struct{}, T]` subscribed to with
`struct{}{}`: the first subscriber opens it and the last one to leave stops it.

**Observability.** `Group.Hooks` reports opens, stops, joins, leaves and drops.

## Guarantees

- **One upstream per key.** Concurrent subscribers of a key open it once, and it
  is stopped once.
- **Open and stop are serialized.** A key is never re-opened before its previous
  upstream has been stopped.
- **In order, one at a time.** Values reach every subscriber in the order they
  were emitted, and a subscriber is never delivered to concurrently, even when
  the Source emits from several goroutines.
- **Stored before delivered.** Once a key has a sampler, a value emitted to it
  becomes its newest before it is delivered to anyone, so a `SubscribeFunc`
  function woken by a value reads that same value from `Latest`. That is what
  lets one subscription carry the state and another the edge. It holds for
  values delivered as they are emitted, not for what `Replay` sends a
  subscriber as it joins, which `Latest` may already have moved past.
- **Nothing after Close.** Once `Close` returns, its subscriber is never
  delivered to again. A delivery in progress completes first.
- **Nothing after the end.** Values emitted after the upstream is stopped or has
  ended are dropped.
- **Keys are independent.** Opening or stopping one key never waits for
  another, and neither does a subscriber that has fallen behind, except inside
  `Group.Close`, which stops keys one at a time, and while a `Joined` or `Left`
  hook runs under the lock the whole Group shares.

## Rules

- Every subscriber receives the **same** value. Treat it as read-only.
- A `SubscribeFunc` function, `Initial`, `Now` and the `Dropped` hook run under
  the key's lock, as a delivery. They may read `Latest`, `Err` and `Dropped`,
  and a `SubscribeFunc` function or `Initial` may `Emit` on another key to feed
  a stream derived from this one, unless what that key delivers leads back to
  this one. They must not subscribe or `Close` a subscription on any key,
  since either can run a `Source` or `stop` that needs this key, nor `Wait`,
  `Emit` or `End` on the same key, or close the Group: each would wait on the
  lock they hold.
- A `Source`, its `stop`, `ReplayFor` and any goroutine they own **may** use
  the Group: they can subscribe to other keys and close any subscription, which
  is what a stream derived from another one needs. They must not subscribe to
  their own key or close the Group — a key is held from the moment it starts
  opening until its `stop` has returned, so either call would wait for itself.
- Hooks must not call back into the Group. `Joined` and `Left` run under a lock
  shared by the whole Group, so keep them short.
- A panic in your code fails the call it ran in. The Group is not left locked
  and nobody is left waiting on a key. A key whose `Opened`, `Joined` or `Left`
  hook panicked may run until its next subscriber leaves or the Group is
  closed, and whatever a `Source` or `stop` had started and not stopped when it
  panicked, the Group never stops. On a goroutine the package starts — the
  timer that stops a key once its `Linger` runs out, which calls `stop` and
  `Stopped`, or the one `Run` and `Poll` call their function on — there is no
  call of yours to fail, and a panic crashes the program unless your function
  recovers it.
- Always `Close` a subscription, even one that has already ended.

## Performance

`go test -run='^$' -bench=. -benchmem` on an Apple M4 Pro, Go 1.26.4.

**Sharing.** One 4 KiB message per op, each decoded by copying and checksumming
it. A dedicated upstream per subscriber decodes it once per subscriber; a
shared one decodes it once.

| Subscribers | Dedicated | Shared | |
|---:|---:|---:|---:|
| 1 | 731 ns | 784 ns | 1× |
| 10 | 7.2 µs | 0.77 µs | 9.4× |
| 100 | 72 µs | 0.92 µs | 79× |

**Emit**, one value to every subscriber of a key. No allocation in any case.

| Subscribers | `SubscribeFunc` | `Subscribe`, read by a goroutine | `Subscribe`, full (`DropOldest`) |
|---:|---:|---:|---:|
| 1 | 5.5 ns | 36 ns | 24 ns |
| 10 | 23 ns | 484 ns | 213 ns |
| 100 | 194 ns | 15.2 µs | 2.5 µs |

A channel costs most when its reader is parked: each send wakes a goroutine.
Prefer `SubscribeFunc` for high fan-out on a hot path.

**Sampling**, where the key keeps one value however many subscribers read it.
`Latest()` itself is 5.1 ns and allocates nothing.

| Subscribers | `Subscribe`, read by a goroutine | `SubscribeLatest` | |
|---:|---:|---:|---:|
| 1 | 37 ns | 39 ns | 1× |
| 10 | 463 ns | 47 ns | 10× |
| 100 | 14.7 µs | 182 ns | 80× |

**Independence.** Opening keys that share nothing, with a `Source` that takes
1 ms — roughly what subscribing to a broker costs. The total is one delay, not
one per key.

| Keys opened at once | 1 | 8 | 32 |
|---|---:|---:|---:|
| Total | 1.19 ms | 1.28 ms | 1.24 ms |

**Lifecycle.**

| | Time | Allocations |
|---|---:|---:|
| Join and leave an open key, `SubscribeFunc` | 78 ns | 2 |
| Join and leave an open key, `Subscribe` | 103 ns | 3 |
| Join with `Replay: 8` to catch up on | 206 ns | 3 |
| Open and stop a key | 198 ns | 5 |
| Emit from 12 goroutines at once, 10 subscribers | 112 ns | 0 |

## Development

```sh
# Check formatting, vet, race-test five times, and fail below 100% coverage.
$ ./scripts/test.sh

# Race-test more, when chasing something intermittent.
$ STREAMFLIGHT_TEST_COUNT=50 ./scripts/test.sh

# The same, in the container CI uses.
$ docker buildx bake test
```

CI runs that container on `linux/amd64` and `linux/arm64`.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT
