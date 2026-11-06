package main

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp"
	"google.golang.org/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

const (
	defaultAlertsQueue = "contester.printeralerts"
	defaultAlertGrace  = 20 * time.Second
	alertPollInterval  = 5 * time.Second
)

// Conditions that keep a printer from printing until someone does something.
// Low toner, warming up, busy and the like are not among them.
const (
	blockingPrinterBits = printerPaused | printerError | printerPendingDeletion | printerPaperJam |
		printerPaperOut | printerPaperProblem | printerOffline | printerOutputBinFull |
		printerNotAvailable | printerUserIntervention | printerOutOfMemory | printerDoorOpen |
		printerServerUnknown | printerNoToner
	blockingJobBits = jobError | jobOffline | jobPaperOut | jobBlockedDevq | jobUserIntervention
	finishedJobBits = jobPrinted | jobComplete | jobDeleted | jobDeleting
)

// blockingCondition says why the printer can't print right now, for people,
// or returns "" if nothing is in the way. The queue is looked at too, since
// some drivers report a problem only on the job.
func blockingCondition(p printerInfo, jobs []jobInfo) string {
	var names []string
	seen := map[string]bool{}
	add := func(v uint32, table []bitName) {
		for _, n := range table {
			if v&n.bit != 0 && !seen[n.name] {
				seen[n.name] = true
				names = append(names, n.name)
			}
		}
	}
	if p.Attributes&printerAttrWorkOffline != 0 {
		names = append(names, "set to \"Use Printer Offline\"")
	}
	add(p.Status&blockingPrinterBits, printerBits)
	var fromJobs uint32
	for _, j := range jobs {
		if j.Status&finishedJobBits == 0 {
			fromJobs |= j.Status & blockingJobBits
		}
	}
	add(fromJobs, jobBits)
	return strings.Join(names, ", ")
}

// unfinished counts the jobs in the queue that haven't been printed yet.
func unfinished(jobs []jobInfo) int {
	n := 0
	for _, j := range jobs {
		if j.Status&finishedJobBits == 0 {
			n++
		}
	}
	return n
}

// alertChange is something to tell the web application.
type alertChange struct {
	Open      bool
	Condition string
	Since     time.Time
}

// alertTracker turns observations of one printer into changes: a condition
// has to last for grace before it is reported, so that a quickly fixed
// problem is not. After that it reports every change of the condition and
// when the condition is gone.
type alertTracker struct {
	grace time.Duration
	cond  string
	since time.Time
	open  bool
}

// observe takes the current condition ("" for none). Only active printers
// (the ones with jobs waiting) are reported when the problem starts.
func (a *alertTracker) observe(now time.Time, cond string, active bool) *alertChange {
	if cond == "" {
		wasOpen := a.open
		*a = alertTracker{grace: a.grace}
		if wasOpen {
			return &alertChange{}
		}
		return nil
	}
	changed := cond != a.cond
	if a.cond == "" {
		a.since = now
	}
	a.cond = cond
	if !active {
		return nil
	}
	if a.open {
		if changed {
			return &alertChange{Open: true, Condition: cond, Since: a.since}
		}
		return nil
	}
	if now.Sub(a.since) >= a.grace {
		a.open = true
		return &alertChange{Open: true, Condition: cond, Since: a.since}
	}
	return nil
}

// settled says that nothing more will come of this printer: it has no
// problem, or the problem was there with nobody waiting.
func (a *alertTracker) settled(now time.Time) bool {
	if a.open {
		return false
	}
	return a.cond == "" || now.Sub(a.since) >= a.grace
}

type watchedPrinter struct {
	inflight int
	tracker  alertTracker
}

// alertMonitor watches the printers that have jobs on them, and printers
// with an open problem until it is gone.
type alertMonitor struct {
	spool spooler
	agent string
	grace time.Duration
	send  func(*tpb.PrinterAlert)

	mu       sync.Mutex
	printers map[string]*watchedPrinter
}

func newAlertMonitor(sp spooler, agent string, grace time.Duration, send func(*tpb.PrinterAlert)) *alertMonitor {
	return &alertMonitor{spool: sp, agent: agent, grace: grace, send: send, printers: make(map[string]*watchedPrinter)}
}

// begin and end bracket the handling of one job. Both work on a nil monitor.
func (m *alertMonitor) begin(printer string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.printers[printer]
	if w == nil {
		w = &watchedPrinter{tracker: alertTracker{grace: m.grace}}
		m.printers[printer] = w
	}
	w.inflight++
}

func (m *alertMonitor) end(printer string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if w := m.printers[printer]; w != nil && w.inflight > 0 {
		w.inflight--
	}
}

func (m *alertMonitor) run(ctx context.Context) {
	t := time.NewTicker(alertPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.tick(now)
		}
	}
}

// tick looks at every watched printer once.
func (m *alertMonitor) tick(now time.Time) {
	m.mu.Lock()
	names := make([]string, 0, len(m.printers))
	for name := range m.printers {
		names = append(names, name)
	}
	m.mu.Unlock()
	sort.Strings(names)

	for _, name := range names {
		pi, err := m.spool.Printer(name)
		if err != nil {
			log.Debugf("Alerts: can't read printer %q: %v", name, err)
			continue
		}
		jobs, err := m.spool.Jobs(name)
		if err != nil {
			log.Debugf("Alerts: can't list jobs of %q: %v", name, err)
			continue
		}
		waiting := unfinished(jobs)

		m.mu.Lock()
		w := m.printers[name]
		if w == nil {
			m.mu.Unlock()
			continue
		}
		change := w.tracker.observe(now, blockingCondition(pi, jobs), w.inflight > 0)
		if w.inflight == 0 && w.tracker.settled(now) {
			delete(m.printers, name)
		}
		m.mu.Unlock()

		if change != nil {
			a := &tpb.PrinterAlert{
				Agent:            m.agent,
				Printer:          name,
				Open:             change.Open,
				Condition:        change.Condition,
				TimestampSeconds: now.Unix(),
			}
			if change.Open {
				a.WaitingJobs = uint32(waiting)
				a.SinceSeconds = change.Since.Unix()
			}
			log.Infof("Printer %q: open=%v %s", name, a.Open, a.Condition)
			m.send(a)
		}
	}
}

func (s *server) alertGrace() time.Duration {
	if s.AlertGraceSeconds > 0 {
		return time.Duration(s.AlertGraceSeconds) * time.Second
	}
	return defaultAlertGrace
}

// startAlerts reports printer problems to the web application until ctx ends.
func (s *server) startAlerts(ctx context.Context, conn *stomp.Conn) {
	queue := s.AlertsQueue
	if queue == "" {
		queue = defaultAlertsQueue
	}
	s.alerts = newAlertMonitor(s.spool, s.agentName(), s.alertGrace(), func(a *tpb.PrinterAlert) {
		body, err := proto.Marshal(a)
		if err != nil {
			log.Errorf("Can't marshal printer alert: %v", err)
			return
		}
		if err := conn.Send(queue, "application/vnd.google.protobuf", body, stomp.SendOpt.Header("delivery-mode", "2")); err != nil {
			log.Errorf("Can't send printer alert: %v", err)
		}
	})
	go s.alerts.run(ctx)
}
