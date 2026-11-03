package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// Win32 spooler status bits (winspool.h).
const (
	jobPaused           = 0x1
	jobError            = 0x2
	jobDeleting         = 0x4
	jobSpooling         = 0x8
	jobPrinting         = 0x10
	jobOffline          = 0x20
	jobPaperOut         = 0x40
	jobPrinted          = 0x80
	jobDeleted          = 0x100
	jobBlockedDevq      = 0x200
	jobUserIntervention = 0x400
	jobRestart          = 0x800
	jobComplete         = 0x1000
	jobRetained         = 0x2000
	jobRenderingLocally = 0x4000

	printerPaused           = 0x1
	printerError            = 0x2
	printerPendingDeletion  = 0x4
	printerPaperJam         = 0x8
	printerPaperOut         = 0x10
	printerManualFeed       = 0x20
	printerPaperProblem     = 0x40
	printerOffline          = 0x80
	printerIOActive         = 0x100
	printerBusy             = 0x200
	printerPrinting         = 0x400
	printerOutputBinFull    = 0x800
	printerNotAvailable     = 0x1000
	printerWaiting          = 0x2000
	printerProcessing       = 0x4000
	printerInitializing     = 0x8000
	printerWarmingUp        = 0x10000
	printerTonerLow         = 0x20000
	printerNoToner          = 0x40000
	printerPagePunt         = 0x80000
	printerUserIntervention = 0x100000
	printerOutOfMemory      = 0x200000
	printerDoorOpen         = 0x400000
	printerServerUnknown    = 0x800000
	printerPowerSave        = 0x1000000

	printerAttrWorkOffline = 0x400
)

type bitName struct {
	bit  uint32
	name string
}

var jobBits = []bitName{
	{jobPaused, "paused"}, {jobError, "error"}, {jobDeleting, "deleting"},
	{jobSpooling, "spooling"}, {jobPrinting, "printing"}, {jobOffline, "offline"},
	{jobPaperOut, "paper out"}, {jobPrinted, "printed"}, {jobDeleted, "deleted"},
	{jobBlockedDevq, "blocked"}, {jobUserIntervention, "user intervention"},
	{jobRestart, "restart"}, {jobComplete, "complete"}, {jobRetained, "retained"},
	{jobRenderingLocally, "rendering locally"},
}

var printerBits = []bitName{
	{printerPaused, "paused"}, {printerError, "error"}, {printerPendingDeletion, "pending deletion"},
	{printerPaperJam, "paper jam"}, {printerPaperOut, "paper out"}, {printerManualFeed, "manual feed"},
	{printerPaperProblem, "paper problem"}, {printerOffline, "offline"}, {printerIOActive, "io active"},
	{printerBusy, "busy"}, {printerPrinting, "printing"}, {printerOutputBinFull, "output bin full"},
	{printerNotAvailable, "not available"}, {printerWaiting, "waiting"}, {printerProcessing, "processing"},
	{printerInitializing, "initializing"}, {printerWarmingUp, "warming up"}, {printerTonerLow, "toner low"},
	{printerNoToner, "no toner"}, {printerPagePunt, "page punt"}, {printerUserIntervention, "user intervention"},
	{printerOutOfMemory, "out of memory"}, {printerDoorOpen, "door open"},
	{printerServerUnknown, "server unknown"}, {printerPowerSave, "power save"},
}

func describeBits(v uint32, names []bitName) string {
	var parts []string
	for _, n := range names {
		if v&n.bit != 0 {
			parts = append(parts, n.name)
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

type jobInfo struct {
	ID           uint32
	Document     string
	Status       uint32
	TotalPages   uint32
	PagesPrinted uint32
}

type printerInfo struct {
	Status     uint32
	Attributes uint32
	Driver     string
}

// spooler is the part of the Windows print spooler the agent looks at.
// Both calls take the printer name exactly as it is given in the job.
type spooler interface {
	Printer(name string) (printerInfo, error)
	Jobs(name string) ([]jobInfo, error)
}

// printerProblem returns a reason why a job sent to this printer would
// certainly not be printed, or "" if it looks usable. A paused queue counts,
// since someone paused it on purpose. Conditions that an operator can fix
// while the job waits (paper, toner, jam) are not problems here; the job
// watcher reports them if they last.
func printerProblem(p printerInfo) string {
	const fatal = printerOffline | printerError | printerNotAvailable |
		printerPendingDeletion | printerServerUnknown | printerPaused
	if p.Attributes&printerAttrWorkOffline != 0 {
		return "printer is set to \"Use Printer Offline\""
	}
	if p.Status&fatal != 0 {
		return "printer status: " + describeBits(p.Status&fatal, printerBits)
	}
	return ""
}

type jobOutcome struct {
	JobID        uint32
	Observed     bool // false if the job was never seen in the queue
	Printed      bool
	PagesPrinted uint32
	Message      string // what went wrong, empty if Printed
}

// jobWatcher follows the one job that a print command created. The command
// doesn't tell us the spooler job id, so the job is found by comparing the
// queue with a baseline taken before the command started: the document name
// has to contain docName, or, once the command is done, there has to be
// exactly one new job.
type jobWatcher struct {
	mu       sync.Mutex // observe runs in the poller, know in the submitter
	baseline map[uint32]bool
	docName  string

	found   bool
	last    jobInfo
	printed bool
	absent  int // consecutive polls after submission with no candidate
}

func newJobWatcher(baseline []jobInfo, docName string) *jobWatcher {
	w := &jobWatcher{baseline: make(map[uint32]bool), docName: docName}
	for _, j := range baseline {
		w.baseline[j.ID] = true
	}
	return w
}

const absentPolls = 3

// observe takes the current queue contents and reports whether the outcome
// is final. submitted says the print command has already exited.
func (w *jobWatcher) observe(jobs []jobInfo, submitted bool) (bool, jobOutcome) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.found {
		if done, out := w.find(jobs, submitted); !w.found {
			return done, out
		}
	}
	for _, j := range jobs {
		if j.ID != w.last.ID {
			continue
		}
		w.last = j
		if j.Status&(jobPrinted|jobComplete) != 0 {
			w.printed = true
			return true, w.outcome(true, "")
		}
		if j.Status&jobDeleted != 0 {
			return true, w.outcome(false, "job was deleted from the print queue")
		}
		return false, jobOutcome{}
	}
	// The job left the queue.
	if w.last.Status&(jobDeleting|jobDeleted) != 0 && !w.printed {
		return true, w.outcome(false, "job was deleted from the print queue")
	}
	return true, w.outcome(true, "")
}

// find looks for the job among the new entries of the queue and adopts it.
// If it can't be found and it's time to give up, it returns a final outcome.
func (w *jobWatcher) find(jobs []jobInfo, submitted bool) (bool, jobOutcome) {
	var fresh []jobInfo
	for _, j := range jobs {
		if w.baseline[j.ID] {
			continue
		}
		fresh = append(fresh, j)
		if strings.Contains(j.Document, w.docName) {
			w.adopt(j, "document name matches")
			return false, jobOutcome{}
		}
	}
	if !submitted {
		return false, jobOutcome{}
	}
	if len(fresh) == 1 {
		w.adopt(fresh[0], "only new job in the queue")
		return false, jobOutcome{}
	}
	if len(fresh) > 1 {
		// Ambiguous; keep looking for a name match until the timeout.
		log.Warnf("%d new jobs in the queue, none named like %q", len(fresh), w.docName)
		w.absent = 0
		return false, jobOutcome{}
	}
	if w.absent++; w.absent >= absentPolls {
		// Either the printer was fast enough that the job came and went
		// between two polls, or it never reached the queue. A false "printed"
		// is worse than a false failure, so report an error.
		log.Warnf("No new job like %q appeared in the queue; queue is: %s", w.docName, describeJobs(jobs))
		return true, jobOutcome{Message: "print job never appeared in the print queue"}
	}
	return false, jobOutcome{}
}

// know tells the watcher the id of the job once the submitter learns it,
// which saves guessing from the queue.
func (w *jobWatcher) know(id uint32, docName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.found {
		if w.last.ID != id {
			log.Warnf("Tracking spooler job %d, but the submitter says %d", w.last.ID, id)
		}
		return
	}
	w.adopt(jobInfo{ID: id, Document: docName}, "job id known")
}

func (w *jobWatcher) adopt(j jobInfo, why string) {
	log.Infof("Tracking spooler job %d %q (%s)", j.ID, j.Document, why)
	w.found = true
	w.last = j
}

func (w *jobWatcher) outcome(printed bool, msg string) jobOutcome {
	return jobOutcome{
		JobID:        w.last.ID,
		Observed:     true,
		Printed:      printed,
		PagesPrinted: w.last.PagesPrinted,
		Message:      msg,
	}
}

func (w *jobWatcher) timeout() jobOutcome {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.found {
		return jobOutcome{Message: "print job did not show up in the print queue"}
	}
	return w.outcome(false, fmt.Sprintf("print job %d did not finish in time; status: %s, pages printed %d of %d",
		w.last.ID, describeBits(w.last.Status, jobBits), w.last.PagesPrinted, w.last.TotalPages))
}

// watchJob polls the queue until the job reaches a final state or ctx ends.
func watchJob(ctx context.Context, sp spooler, printer string, w *jobWatcher, submitted *atomic.Bool, interval time.Duration) jobOutcome {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		jobs, err := sp.Jobs(printer)
		if err != nil {
			log.Warnf("Can't list jobs of %q: %v", printer, err)
		} else if done, out := w.observe(jobs, submitted.Load()); done {
			return out
		}
		select {
		case <-ctx.Done():
			out := w.timeout()
			// A paused or broken printer shows up on the printer, not the job.
			if pi, err := sp.Printer(printer); err == nil && pi.Status != 0 {
				out.Message += "; printer status: " + describeBits(pi.Status, printerBits)
			}
			return out
		case <-t.C:
		}
	}
}

func describeJobs(jobs []jobInfo) string {
	if len(jobs) == 0 {
		return "empty"
	}
	var parts []string
	for _, j := range jobs {
		parts = append(parts, fmt.Sprintf("#%d %q [%s]", j.ID, j.Document, describeBits(j.Status, jobBits)))
	}
	return strings.Join(parts, "; ")
}
