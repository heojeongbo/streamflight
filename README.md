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

Then subscribe, with a function called for each value:

```go
sub, err := g.SubscribeFunc("prices/BTC", func(b []byte) { conn.Send(b) })
if err != nil {
	return err
}
defer sub.Close() // the last Close stops the upstream
```

or with a channel:

```go
sub, err := g.Subscribe("prices/BTC", streamflight.WithBuffer(16))
if err != nil {
	return err
}
defer sub.Close()

for b := range sub.C {
	render(b)
}
return sub.Err() // why it ended: io.EOF, the upstream's error, ErrEvicted, ...
```

A Source whose upstream is a repeated request rather than a subscription is
written with `Poll`:

```go
Replay: 1, // the first tick runs before the opening subscriber is attached
Source: streamflight.Poll(3*time.Second,
	func(ctx context.Context, host string, e streamflight.Emitter[Status]) error {
		e.Emit(poll(ctx, host))
		return nil
	}),
```

The first call is on open, so a subscriber sees something immediately, and the
next is an interval after the last one *returned* — not a `time.Ticker`, which
keeps the tick a slow call missed and fires again at once, leaving a poll that
outruns its interval running back to back with no idle at all.

A Source that runs its own loop is written with `Run` instead:

```go
Source: streamflight.Run(func(ctx context.Context, host string, e streamflight.Emitter[Status]) error {
	for {
		v, err := conn.Read(ctx) // whatever blocks until the next value
		if err != nil {
			return err // the upstream ends; subscribers see this error
		}
		e.Emit(v)
	}
}),
```

## Relaying to a client

A handler that relays one key to one client is `Drain`:

```go
func (s *Server) Watch(req *Request, stream grpc.ServerStreamingServer[Status]) error {
	sub, err := s.group.Subscribe(req.Key, streamflight.WithBuffer(16))
	if err != nil {
		return err
	}
	defer sub.Close()
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

## Choosing the behaviour

**Delivery.** Three shapes, for three kinds of consumer:

| | | For |
|---|---|---|
| `SubscribeFunc` | calls a function on the emitting goroutine | every value matters and the work is short |
| `Subscribe` | queues on a channel | every value matters and the work can block |
| `SubscribeLatest` | keeps only the newest, read with `Latest()` | only the current value matters |

`SubscribeFunc` queues nothing, but a slow function holds up every other
subscriber of the key. `Subscribe` queues instead, and its `Overflow` policy
decides what happens to a subscriber that falls behind:

| Policy | A full queue… | For |
|---|---|---|
| `DropOldest` (default) | discards its oldest value | state, where only the latest matters |
| `DropNewest` | refuses the arriving value | keeping the start of a burst |
| `Block` | waits for the subscriber, and so does everyone else | lossless delivery to consumers that keep up |
| `Evict` | closes the subscriber with `ErrEvicted` | deltas, where a gap corrupts everything after it |

**Late subscribers.** `Group.Replay` sends a joining subscriber the latest
values, for state that is only published when it changes — or `Group.ReplayFor`
when only some keys are state, so that a topic wanting 1 and an event stream
wanting 0 can share one Group. `Group.Initial` sends it anything else first,
such as a snapshot for a stream of deltas.

**Churn.** `Group.Linger` keeps an upstream running for a while after its last
subscriber leaves, so a reloaded page reuses it instead of opening it again.

**Upstreams that end.** A Source calls `Emitter.End` when its upstream ends by
itself. Every subscriber is closed with that error, and the next subscriber
opens a fresh upstream.

**Observability.** `Group.Hooks` reports opens, stops, joins, leaves and drops.

## Guarantees

- **One upstream per key.** Concurrent subscribers of a key open it once, and it
  is stopped once.
- **Open and stop are serialized.** A key is never re-opened before its previous
  upstream has been stopped.
- **In order, one at a time.** Values reach every subscriber in the order they
  were emitted, and a subscriber is never delivered to concurrently, even when
  the Source emits from several goroutines.
- **Nothing after Close.** Once `Close` returns, its subscriber is never
  delivered to again.
- **Keys are independent.** Opening or stopping one key never blocks another,
  and neither does a subscriber that has fallen behind.

## Rules

- Every subscriber receives the **same** value. Treat it as read-only.
- Delivery functions, `Initial` and `Hooks` must not call back into the Group,
  and a `SubscribeFunc` function must not close its own subscription.
- A `Source`, its `stop` and any goroutine they own **may** use the Group: they
  can subscribe to other keys and close any subscription, which is what a
  stream derived from another one needs. They must not subscribe to their own
  key or close the Group — a key is held from the moment it starts opening
  until its `stop` has returned, so either call would wait for itself.
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
