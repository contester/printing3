package main

import (
	"time"

	tpb "github.com/contester/printing3/tickets"
)

// renderedReport is what the web application learns when a job has been
// rendered: it keeps the rendered bytes so that a later reprint can skip the
// render.
func renderedReport(job *tpb.BinaryJob) *tpb.PrintJobReport {
	return &tpb.PrintJobReport{
		JobExpandedId:    job.GetJobId(),
		Stage:            tpb.PrintJobReport_RENDERED,
		NumPages:         job.GetPages(),
		TimestampSeconds: time.Now().Unix(),
		Render:           job,
	}
}
