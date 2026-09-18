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

A Source that runs a loop, such as a poller, is written with `Run`:

```go
Source: streamflight.Run(func(ctx context.Context, host string, e streamflight.Emitter[Status]) error {
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done(): // the last subscriber left
			return nil
		case <-tick.C:
			e.Emit(poll(host))
		}
	}
}),
```

## Choosing the behaviour

**Delivery.** `SubscribeFunc` calls the function on the goroutine that emitted
the value: nothing is queued, but a slow function holds up every other
subscriber of the key. `Subscribe` queues on a channel instead, and its
`Overflow` policy decides what happens to a subscriber that falls behind:

| Policy | A full queue… | For |
|---|---|---|
| `DropOldest` (default) | discards its oldest value | state, where only the latest matters |
| `DropNewest` | refuses the arriving value | keeping the start of a burst |
| `Block` | waits for the subscriber, and so does everyone else | lossless delivery to consumers that keep up |
| `Evict` | closes the subscriber with `ErrEvicted` | deltas, where a gap corrupts everything after it |

**Late subscribers.** `Group.Replay` sends a joining subscriber the latest
values, for state that is only published when it changes. `Group.Initial` sends
it anything else first, such as a snapshot for a stream of deltas.

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

## Rules

- Every subscriber receives the **same** value. Treat it as read-only.
- Delivery functions, `Initial` and `Hooks` must not call back into the Group,
  and a `SubscribeFunc` function must not close its own subscription.
- Always `Close` a subscription, even one that has already ended.

## Performance

`go test -run='^$' -bench=. -benchmem` on an Apple M3 Max, Go 1.26.4.

**Sharing.** One 4 KiB message per op, each decoded by copying and checksumming
it. A dedicated upstream per subscriber decodes it once per subscriber; a
shared one decodes it once.

| Subscribers | Dedicated | Shared | |
|---:|---:|---:|---:|
| 1 | 745 ns | 766 ns | 1× |
| 10 | 7.6 µs | 0.80 µs | 9.5× |
| 100 | 77 µs | 0.99 µs | 78× |

**Emit**, one value to every subscriber of a key. No allocation in any case.

| Subscribers | `SubscribeFunc` | `Subscribe`, read by a goroutine | `Subscribe`, full (`DropOldest`) |
|---:|---:|---:|---:|
| 1 | 6.7 ns | 49 ns | 28 ns |
| 10 | 27 ns | 574 ns | 247 ns |
| 100 | 224 ns | 12.3 µs | 2.4 µs |

A channel costs most when its reader is parked: each send wakes a goroutine.
Prefer `SubscribeFunc` for high fan-out on a hot path.

**Lifecycle.**

| | Time | Allocations |
|---|---:|---:|
| Join and leave an open key, `SubscribeFunc` | 109 ns | 3 |
| Join and leave an open key, `Subscribe` | 151 ns | 5 |
| Open and stop a key | 205 ns | 6 |
| Emit from 14 goroutines at once, 10 subscribers | 147 ns | 0 |

## Development

```sh
# Vet, race-test, and fail below 100% coverage.
$ ./scripts/test.sh

# The same, in the container CI uses.
$ docker buildx bake test
```

## License

MIT
