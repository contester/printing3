package tools

import "sync"

// Keyed runs functions so that those with the same key run one at a time, in
// the order they were given, while functions with different keys run
// concurrently. A key's goroutine exists only while it has work.
type Keyed struct {
	mu    sync.Mutex
	lanes map[string][]func() // a key is present while its goroutine runs
}

// Go queues fn behind everything already queued under key. It never blocks.
func (k *Keyed) Go(key string, fn func()) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.lanes == nil {
		k.lanes = make(map[string][]func())
	}
	q, running := k.lanes[key]
	k.lanes[key] = append(q, fn)
	if !running {
		go k.run(key)
	}
}

func (k *Keyed) run(key string) {
	for {
		k.mu.Lock()
		q := k.lanes[key]
		if len(q) == 0 {
			delete(k.lanes, key)
			k.mu.Unlock()
			return
		}
		fn := q[0]
		q[0] = nil
		k.lanes[key] = q[1:]
		k.mu.Unlock()
		fn()
	}
}
