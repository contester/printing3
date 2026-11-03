package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrinterProblem(t *testing.T) {
	if p := printerProblem(printerInfo{Status: printerTonerLow | printerPaperOut | printerBusy}); p != "" {
		t.Errorf("fixable conditions should not block: %q", p)
	}
	for _, p := range []printerInfo{
		{Status: printerOffline},
		{Status: printerPaused},
		{Status: printerNotAvailable | printerBusy},
		{Attributes: printerAttrWorkOffline},
	} {
		if printerProblem(p) == "" {
			t.Errorf("%+v should be a problem", p)
		}
	}
}

func TestWatcherNameMatch(t *testing.T) {
	w := newJobWatcher([]jobInfo{{ID: 1, Document: "old"}}, "job-7.ps")
	steps := []struct {
		jobs      []jobInfo
		submitted bool
		done      bool
		printed   bool
	}{
		{[]jobInfo{{ID: 1}}, false, false, false},
		{[]jobInfo{{ID: 1}, {ID: 2, Document: "other"}, {ID: 3, Document: "c:\\w\\job-7.ps", Status: jobSpooling}}, false, false, false},
		{[]jobInfo{{ID: 1}, {ID: 2, Document: "other"}, {ID: 3, Document: "c:\\w\\job-7.ps", Status: jobPrinting, PagesPrinted: 1}}, true, false, false},
		{[]jobInfo{{ID: 1}, {ID: 2, Document: "other"}}, true, true, true},
	}
	for i, s := range steps {
		done, out := w.observe(s.jobs, s.submitted)
		if done != s.done || (done && out.Printed != s.printed) {
			t.Fatalf("step %d: done=%v out=%+v", i, done, out)
		}
		if done && (out.JobID != 3 || !out.Observed) {
			t.Errorf("step %d: wrong job %+v", i, out)
		}
	}
}

func TestWatcherOnlyNewJobAfterSubmit(t *testing.T) {
	w := newJobWatcher(nil, "job-7.ps")
	jobs := []jobInfo{{ID: 5, Document: "Ghostscript output", Status: jobSpooling}}
	if done, _ := w.observe(jobs, false); done {
		t.Fatal("must not adopt an unnamed job before the command is done")
	}
	if done, _ := w.observe(jobs, true); done || !w.found || w.last.ID != 5 {
		t.Fatalf("should adopt the only new job: found=%v", w.found)
	}
	jobs[0].Status = jobPrinted
	if done, out := w.observe(jobs, true); !done || !out.Printed {
		t.Fatalf("got %v %+v", done, out)
	}
}

func TestWatcherDeleted(t *testing.T) {
	w := newJobWatcher(nil, "x.ps")
	w.observe([]jobInfo{{ID: 2, Document: "x.ps", Status: jobPrinting}}, true)
	if done, out := w.observe([]jobInfo{{ID: 2, Document: "x.ps", Status: jobDeleting}}, true); done {
		t.Fatalf("deleting is not final yet: %+v", out)
	}
	done, out := w.observe(nil, true)
	if !done || out.Printed || out.Message == "" {
		t.Fatalf("got %v %+v", done, out)
	}
}

func TestWatcherNeverAppears(t *testing.T) {
	w := newJobWatcher(nil, "x.ps")
	for i := 0; i < absentPolls-1; i++ {
		if done, _ := w.observe(nil, true); done {
			t.Fatal("gave up too early")
		}
	}
	if done, out := w.observe(nil, true); !done || out.Observed || out.Printed || out.Message == "" {
		t.Fatalf("got %v %+v", done, out)
	}
}

type fakeSpooler struct{ jobs atomic.Value }

func (f *fakeSpooler) Printer(string) (printerInfo, error) { return printerInfo{}, nil }
func (f *fakeSpooler) Jobs(string) ([]jobInfo, error)      { return f.jobs.Load().([]jobInfo), nil }

func TestWatchJobTimeout(t *testing.T) {
	f := &fakeSpooler{}
	f.jobs.Store([]jobInfo{{ID: 9, Document: "x.ps", Status: jobError | jobPaperOut, TotalPages: 3}})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var submitted atomic.Bool
	submitted.Store(true)
	out := watchJob(ctx, f, "p", newJobWatcher(nil, "x.ps"), &submitted, 5*time.Millisecond)
	if out.Printed || out.JobID != 9 || out.Message == "" {
		t.Fatalf("got %+v", out)
	}
	t.Log(out.Message)
}

func TestWatcherKnownID(t *testing.T) {
	w := newJobWatcher(nil, "x.pdf")
	w.know(7, "x.pdf")
	// Name doesn't match, but the id is known.
	if done, _ := w.observe([]jobInfo{{ID: 7, Document: "other", Status: jobSpooling}}, false); done {
		t.Fatal("still spooling")
	}
	if done, out := w.observe(nil, true); !done || !out.Printed || out.JobID != 7 {
		t.Fatalf("got %v %+v", done, out)
	}
}
