package tools

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedSameKeyInOrderOneAtATime(t *testing.T) {
	var k Keyed
	var mu sync.Mutex
	var got []int
	active := 0
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		k.Go("a", func() {
			defer wg.Done()
			mu.Lock()
			active++
			if active != 1 {
				t.Errorf("job %d overlapped another job of the same key", i)
			}
			got = append(got, i)
			mu.Unlock()
			time.Sleep(100 * time.Microsecond)
			mu.Lock()
			active--
			mu.Unlock()
		})
	}
	wg.Wait()
	for i, v := range got {
		if v != i {
			t.Fatalf("out of order: %v", got)
		}
	}
}

func TestKeyedStuckKeyDoesNotBlockOthers(t *testing.T) {
	var k Keyed
	release := make(chan struct{})
	var behind atomic.Bool
	behindDone := make(chan struct{})
	k.Go("stuck", func() { <-release })
	k.Go("stuck", func() { behind.Store(true); close(behindDone) })

	done := make(chan struct{})
	k.Go("other", func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job of another key was blocked")
	}
	if behind.Load() {
		t.Fatal("ran behind a stuck job of the same key")
	}
	close(release)
	<-behindDone
}

func TestKeyedCleansUp(t *testing.T) {
	var k Keyed
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		k.Go("a", wg.Done)
	}
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for {
		k.mu.Lock()
		n := len(k.lanes)
		k.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d lanes left", n)
		}
		time.Sleep(time.Millisecond)
	}
	// A key is reusable after it was cleaned up.
	done := make(chan struct{})
	k.Go("a", func() { close(done) })
	<-done
}
