package main

import (
	"context"
	"os"
	"sort"
	"time"

	"github.com/go-stomp/stomp"
	"google.golang.org/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

const (
	defaultPrintersQueue    = "contester.printers"
	defaultPrintersInterval = 5 * time.Minute
)

// printerList is the message that tells the web application which printers
// this agent can print to.
func printerList(agent string, now time.Time, printers []remotePrinter) *tpb.PrinterList {
	sorted := append([]remotePrinter(nil), printers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].UNC < sorted[j].UNC })
	res := &tpb.PrinterList{Agent: agent, TimestampSeconds: now.Unix()}
	for _, p := range sorted {
		res.Printers = append(res.Printers, &tpb.RemotePrinter{Unc: p.UNC, Server: p.Server, Share: p.Share})
	}
	return res
}

func (s *server) agentName() string {
	if s.AgentName != "" {
		return s.AgentName
	}
	h, err := os.Hostname()
	if err != nil {
		return "printagent"
	}
	return h
}

// publishPrinters sends the printer list now and then, until ctx ends. A
// failure to list the printers is only logged: the web application keeps
// the last list it got.
func (s *server) publishPrinters(ctx context.Context, conn *stomp.Conn) {
	queue := s.PrintersQueue
	if queue == "" {
		queue = defaultPrintersQueue
	}
	interval := defaultPrintersInterval
	if s.PrintersIntervalSeconds > 0 {
		interval = time.Duration(s.PrintersIntervalSeconds) * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if printers, err := s.spool.RemotePrinters(); err != nil {
			log.Warnf("Can't list remote printers: %v", err)
		} else if body, err := proto.Marshal(printerList(s.agentName(), time.Now(), printers)); err != nil {
			log.Errorf("Can't marshal printer list: %v", err)
		} else if err := conn.Send(queue, "application/vnd.google.protobuf", body, stomp.SendOpt.Header("delivery-mode", "2")); err != nil {
			log.Errorf("Can't send printer list: %v", err)
		} else {
			log.Infof("Sent list of %d remote printers to %s", len(printers), queue)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
