package main

import (
	"context"
	"strings"
	"time"

	"git.sgu.ru/sgu/systemdutil"
	"github.com/contester/printing3/tools"
	"github.com/coreos/go-systemd/daemon"
	"github.com/go-stomp/stomp"
	"github.com/gogo/protobuf/proto"
	"github.com/kelseyhightower/envconfig"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

type server struct {
	bconfig

	languageMap map[string]string
}

func (s *server) processPrintJob(ctx context.Context, msg *stomp.Message) error {
	var job tpb.PrintJob

	err := proto.Unmarshal(msg.Body, &job)
	if err != nil {
		log.Errorf("error parsing message %v", msg)
		return tools.MaybeAck(msg)
	}

	bpb := tpb.TexJob{
		Printer: job.GetPrinter(),
		JobId:   job.GetJobId(),
	}

	bpb.Data, err = s.processSource(ctx, &job)
	if err != nil {
		return tools.SendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	return tools.SendAndAck(msg, s.TexQueue, &bpb)
}

func (s *server) processTexJob(ctx context.Context, msg *stomp.Message) error {
	var job tpb.TexJob

	err := proto.Unmarshal(msg.Body, &job)
	if err != nil {
		log.Errorf("error parsing message %v", msg)
		return tools.MaybeAck(msg)
	}

	bpb := tpb.BinaryJob{
		Printer: job.GetPrinter(),
		JobId:   job.GetJobId(),
	}

	bpb.Data, err = s.processTex(ctx, bpb.JobId, job.GetData())
	if err != nil {
		return tools.SendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	return tools.SendAndAck(msg, s.BinaryQueue, &bpb)
}

type bconfig struct {
	StompDSN string

	SourceDir string
	TexDir    string

	SourceQueue  string
	FailureQueue string
	TexQueue     string
	BinaryQueue  string

	Languages []string
}

func main() {
	systemdutil.Init()

	var srv server
	if err := envconfig.Process("busyprint", &srv.bconfig); err != nil {
		log.Fatal(err)
	}

	srv.languageMap = make(map[string]string)
	for _, v := range srv.Languages {
		s := strings.SplitN(v, "=", 2)
		switch len(s) {
		case 1:
			srv.languageMap[s[0]] = s[0]
		case 2:
			srv.languageMap[s[0]] = s[1]
		}
	}

	sconf, err := tools.ParseStompDSN(srv.bconfig.StompDSN)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	sconn, err := tools.DialStomp(ctx, sconf)
	if err != nil {
		log.Fatal(err)
	}

	defer sconn.MustDisconnect()

	sourceSub, err := tools.SubscribeAndProcess(ctx, sconn, srv.SourceQueue, srv.processPrintJob)
	if err != nil {
		log.Fatal(err)
	}
	defer sourceSub.Unsubscribe()

	texSub, err := tools.SubscribeAndProcess(ctx, sconn, srv.TexQueue, srv.processTexJob)
	if err != nil {
		log.Fatal()
	}
	defer texSub.Unsubscribe()
	daemon.SdNotify(false, daemon.SdNotifyReady)
	systemdutil.WaitSigint()
	daemon.SdNotify(false, daemon.SdNotifyStopping)
}
