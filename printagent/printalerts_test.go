package main

import (
	"sync"
	"testing"
	"time"

	tpb "github.com/contester/printing3/tickets"
)

func TestBlockingCondition(t *testing.T) {
	cases := []struct {
		name string
		p    printerInfo
		jobs []jobInfo
		want string
	}{
		{"fine", printerInfo{}, nil, ""},
		{"busy, toner low, warming up are not blocking", printerInfo{Status: printerBusy | printerTonerLow | printerWarmingUp | printerPrinting}, nil, ""},
		{"paper out", printerInfo{Status: printerPaperOut}, nil, "paper out"},
		{"several", printerInfo{Status: printerPaperJam | printerDoorOpen | printerBusy}, nil, "paper jam, door open"},
		{"paused", printerInfo{Status: printerPaused}, nil, "paused"},
		{"use offline", printerInfo{Attributes: printerAttrWorkOffline}, nil, `set to "Use Printer Offline"`},
		{"only the job knows", printerInfo{}, []jobInfo{{Status: jobPaperOut | jobPrinting}}, "paper out"},
		{"printer and job agree", printerInfo{Status: printerPaperOut}, []jobInfo{{Status: jobPaperOut}}, "paper out"},
		{"a finished job's old error is ignored", printerInfo{}, []jobInfo{{Status: jobError | jobComplete}}, ""},
		{"a paused job alone is not it", printerInfo{}, []jobInfo{{Status: jobPaused}}, ""},
	}
	for _, c := range cases {
		if got := blockingCondition(c.p, c.jobs); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestAlertTracker(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	a := alertTracker{grace: 20 * time.Second}

	if c := a.observe(at(0), "paper out", true); c != nil {
		t.Fatalf("reported at once: %+v", c)
	}
	if c := a.observe(at(10), "paper out", true); c != nil {
		t.Fatalf("reported inside the grace period: %+v", c)
	}
	c := a.observe(at(20), "paper out", true)
	if c == nil || !c.Open || c.Condition != "paper out" || !c.Since.Equal(at(0)) {
		t.Fatalf("got %+v", c)
	}
	if c := a.observe(at(25), "paper out", true); c != nil {
		t.Fatalf("repeated: %+v", c)
	}
	// The problem changes while open: tell again, still counting from the start.
	c = a.observe(at(30), "paper out, door open", true)
	if c == nil || !c.Open || c.Condition != "paper out, door open" || !c.Since.Equal(at(0)) {
		t.Fatalf("got %+v", c)
	}
	if a.settled(at(31)) {
		t.Fatal("an open problem is not settled")
	}
	// Nobody is waiting any more, but it is still reported as gone when it is.
	if c := a.observe(at(40), "paper out, door open", false); c != nil {
		t.Fatalf("got %+v", c)
	}
	c = a.observe(at(50), "", false)
	if c == nil || c.Open {
		t.Fatalf("got %+v", c)
	}
	if !a.settled(at(50)) {
		t.Fatal("should be settled")
	}
	if c := a.observe(at(55), "", false); c != nil {
		t.Fatalf("cleared twice: %+v", c)
	}
}

func TestAlertTrackerFixedInTime(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	a := alertTracker{grace: 20 * time.Second}
	a.observe(t0, "paper out", true)
	if c := a.observe(t0.Add(10*time.Second), "", true); c != nil {
		t.Fatalf("nothing was reported, so nothing clears: %+v", c)
	}
	// A new problem starts the clock again.
	if c := a.observe(t0.Add(25*time.Second), "paper out", true); c != nil {
		t.Fatalf("got %+v", c)
	}
}

func TestAlertTrackerNobodyWaiting(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	a := alertTracker{grace: 20 * time.Second}
	if c := a.observe(t0, "offline", false); c != nil {
		t.Fatal(c)
	}
	if c := a.observe(t0.Add(time.Minute), "offline", false); c != nil {
		t.Fatalf("a switched off printer with no jobs is not news: %+v", c)
	}
	if !a.settled(t0.Add(time.Minute)) {
		t.Fatal("no reason to keep watching it")
	}
	// A job arrives after a while; the problem is old, so it is reported now.
	if c := a.observe(t0.Add(2*time.Minute), "offline", true); c == nil || !c.Open {
		t.Fatalf("got %+v", c)
	}
}

type alertSpooler struct {
	mu   sync.Mutex
	info printerInfo
	jobs []jobInfo
}

func (f *alertSpooler) set(p printerInfo, j []jobInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.info, f.jobs = p, j
}
func (f *alertSpooler) Printer(string) (printerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.info, nil
}
func (f *alertSpooler) Jobs(string) ([]jobInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs, nil
}
func (f *alertSpooler) RemotePrinters() ([]remotePrinter, error) { return nil, nil }

func TestAlertMonitor(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	sp := &alertSpooler{}
	var sent []*tpb.PrinterAlert
	m := newAlertMonitor(sp, "agent1", 20*time.Second, func(a *tpb.PrinterAlert) { sent = append(sent, a) })
	const p = `\\srv\p1`

	m.begin(p)
	sp.set(printerInfo{Status: printerPaperOut}, []jobInfo{{ID: 1, Status: jobPrinting}, {ID: 2, Status: jobSpooling}})
	m.tick(t0)
	m.tick(t0.Add(10 * time.Second))
	if len(sent) != 0 {
		t.Fatalf("too early: %v", sent)
	}
	m.tick(t0.Add(20 * time.Second))
	if len(sent) != 1 {
		t.Fatalf("got %v", sent)
	}
	a := sent[0]
	if a.Agent != "agent1" || a.Printer != p || !a.Open || a.Condition != "paper out" || a.WaitingJobs != 2 || a.SinceSeconds != t0.Unix() {
		t.Errorf("got %v", a)
	}

	// The job gave up, but the printer is still broken: keep watching.
	m.end(p)
	m.tick(t0.Add(30 * time.Second))
	if len(sent) != 1 {
		t.Fatalf("repeated: %v", sent)
	}
	m.mu.Lock()
	watching := len(m.printers)
	m.mu.Unlock()
	if watching != 1 {
		t.Fatal("must keep watching a printer with an open problem")
	}

	sp.set(printerInfo{}, nil)
	m.tick(t0.Add(40 * time.Second))
	if len(sent) != 2 || sent[1].Open || sent[1].Condition != "" {
		t.Fatalf("got %v", sent)
	}
	m.mu.Lock()
	watching = len(m.printers)
	m.mu.Unlock()
	if watching != 0 {
		t.Fatal("should have stopped watching")
	}
}

func TestAlertMonitorNilIsSafe(t *testing.T) {
	var m *alertMonitor
	m.begin("p")
	m.end("p")
}
