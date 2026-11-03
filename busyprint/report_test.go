package main

import (
	"testing"

	"google.golang.org/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
)

func TestRenderedReport(t *testing.T) {
	job := &tpb.BinaryJob{
		JobId:   "a-7",
		Printer: "p",
		Pages:   3,
		Payload: &tpb.BinaryJob_Pdf{Pdf: []byte("x")},
	}
	r := renderedReport(job)
	if r.GetJobExpandedId() != "a-7" {
		t.Errorf("job id %q", r.GetJobExpandedId())
	}
	if r.GetStage() != tpb.PrintJobReport_RENDERED {
		t.Errorf("stage %v", r.GetStage())
	}
	if r.GetNumPages() != 3 {
		t.Errorf("pages %d", r.GetNumPages())
	}
	if r.GetTimestampSeconds() <= 0 {
		t.Errorf("timestamp %d", r.GetTimestampSeconds())
	}
	if r.GetErrorMessage() != "" {
		t.Errorf("error %q", r.GetErrorMessage())
	}
	if !proto.Equal(r.GetRender(), job) {
		t.Errorf("render %v, want %v", r.GetRender(), job)
	}
}
