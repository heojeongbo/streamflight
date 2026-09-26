package streamflight

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// help can take a caller off the queue just as that caller gives up. Through
// the exported API only a race gets there, so the queue is set up by hand: a
// caller that has already given up, and a lock that help has to wait for.
func TestHelpPassesOverACallerThatGaveUp(t *testing.T) {
	x := require.New(t)
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	gaveUp := make(chan struct{})
	close(gaveUp)
	f.mu.Lock()
	f.lockersMu.Lock()
	f.lockers = append(f.lockers, &locker{got: make(chan struct{}), gaveUp: gaveUp})
	f.helping = true
	f.lockersMu.Unlock()
	go f.help()
	f.mu.Unlock()

	// Nobody takes the lock from help, so it must pass the caller over, find
	// the queue empty and let the lock go.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		f.lockersMu.Lock()
		helping := f.helping
		f.lockersMu.Unlock()
		if !helping && f.mu.TryLock() {
			f.mu.Unlock()
			break
		}
		x.False(time.Now().After(deadline), "help kept the key's lock")
	}
	x.NoError(s.Close())
}
