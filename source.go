package streamflight

import "context"

// Source opens the upstream of key. It is called once per upstream, by the
// first subscriber of the key, and must deliver values through e until stop is
// called. A nil stop means there is nothing to stop.
//
// Values emitted before Source returns reach the subscriber that is opening the
// key only through [Group.Replay].
type Source[K comparable, T any] func(key K, e Emitter[T]) (stop func() error, err error)

// Emitter is a Source's handle on the subscribers of its key. It is safe for
// concurrent use.
type Emitter[T any] interface {
	// Emit delivers v to every subscriber of the key and returns how many of
	// them accepted it: a function subscriber always does, and a channel
	// subscriber does when v was queued.
	Emit(v T) int

	// End ends the upstream by itself. Every subscriber is closed with err, or
	// io.EOF if err is nil, and the next subscriber of the key opens a fresh
	// upstream. The stop func is still called, once every subscriber has been
	// closed. Emit and End after the first End do nothing.
	End(err error)
}

// Run makes a Source out of a function that produces values until its context
// is done, such as a poller or a loop reading a connection.
//
// run is started on its own goroutine when the key is opened. Stopping the key
// cancels ctx and waits for run to return. If run returns while ctx is still
// live, the upstream ends with the returned error; see [Emitter.End].
func Run[K comparable, T any](run func(ctx context.Context, key K, e Emitter[T]) error) Source[K, T] {
	return func(key K, e Emitter[T]) (func() error, error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			// A no-op once the key is stopping, which is when ctx is done.
			e.End(run(ctx, key, e))
		}()

		return func() error {
			cancel()
			<-done
			return nil
		}, nil
	}
}
