package main

import (
	"context"
	"strings"
	"time"

	"git.sgu.ru/sgu/systemdutil"
	"github.com/contester/printing3/tools"
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

func maybeAck(msg *stomp.Message) error {
	if msg.ShouldAck() {
		return msg.Conn.Ack(msg)
	}
	return nil
}

func sendAndAck(msg *stomp.Message, dest string, data proto.Message) error {
	buf, err := proto.Marshal(data)
	if err != nil {
		log.Errorf("error marshaling message %v: %v", data, err)
		return maybeAck(msg)
	}
	if msg.ShouldAck() {
		tx := msg.Conn.Begin()
		if err := tx.Send(dest, "application/vnd.google.protobuf", buf, stomp.SendOpt.Header("delivery-mode", "2")); err != nil {
			log.Errorf("error sending message %v in transaction: %v", data, err)
			return err
		}
		if err := tx.Ack(msg); err != nil {
			log.Errorf("error acking message in transaction: %v", err)
			return err
		}
		if err := tx.Commit(); err != nil {
			log.Errorf("error committing transaction: %v", err)
			return err
		}
		return nil
	}
	return msg.Conn.Send(dest, "application/vnd.google.protobuf", buf, stomp.SendOpt.Header("delivery-mode", "2"))
}

func (s *server) processPrintJob(ctx context.Context, msg *stomp.Message) error {
	var job tpb.PrintJob

	err := proto.Unmarshal(msg.Body, &job)
	if err != nil {
		log.Errorf("error parsing message %v", msg)
		return maybeAck(msg)
	}

	bpb := tpb.TexJob{
		Printer: job.GetPrinter(),
		JobId:   job.GetJobId(),
	}

	bpb.Data, err = s.processSource(ctx, &job)
	if err != nil {
		return sendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	return sendAndAck(msg, s.TexQueue, &bpb)
}

func (s *server) processTexJob(ctx context.Context, msg *stomp.Message) error {
	var job tpb.TexJob

	err := proto.Unmarshal(msg.Body, &job)
	if err != nil {
		log.Errorf("error parsing message %v", msg)
		return maybeAck(msg)
	}

	bpb := tpb.BinaryJob{
		Printer: job.GetPrinter(),
		JobId:   job.GetJobId(),
	}

	bpb.Data, err = s.processTex(ctx, bpb.JobId, job.GetData())
	if err != nil {
		return sendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	return sendAndAck(msg, s.BinaryQueue, &bpb)
}

func subscribeAndProcess(ctx context.Context, conn *stomp.Conn, queue string, proc func(context.Context, *stomp.Message) error) (*stomp.Subscription, error) {
	sub, err := conn.Subscribe(queue, stomp.AckClient)
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case v, ok := <-sub.C:
			if !ok {
				log.Fatalf("subscription %q closed", sub)
			}
			if err := proc(ctx, v); err != nil {
				log.Fatalf("proc error: %v", err)
			}
		case <-ctx.Done():
			sub.Unsubscribe()
			return
		}
	}()
	return sub, nil
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
			v[s[0]] = s[0]
		case 2:
			v[s[0]] = s[1]
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

	sourceSub, err := subscribeAndProcess(ctx, sconn, srv.SourceQueue, srv.processPrintJob)
	if err != nil {
		log.Fatal(err)
	}
	defer sourceSub.Unsubscribe()

	texSub, err := subscribeAndProcess(ctx, sconn, srv.TexQueue, srv.processTexJob)
	if err != nil {
		log.Fatal()
	}
	defer texSub.Unsubscribe()

	systemdutil.WaitSigint()
}
