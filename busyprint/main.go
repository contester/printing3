package main

import (
	"context"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/gogo/protobuf/proto"
	"github.com/kelseyhightower/envconfig"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

type server struct {
	bconfig

	languages map[string]string
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

	bpb := tpb.BinaryJob{
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

type bconfig struct {
	StompDSN string

	SourceDir string
	TexDir    string

	SourceQueue  string
	FailureQueue string
	TexQueue     string

	Languages []string `envconfig:"LANGUAGES"`
}

func main() {
	var bconf bconfig
	if err := envconfig.Process("busyprint", &bconf); err != nil {
		log.Fatal(err)
	}

	sconf, err := parseStompDSN(bconf.Stomp)
	if err != nil {
		log.Fatal(err)
	}

	sconn, err := sconf.Dial()
	if err != nil {
		log.Fatal(err)
	}

	sourceSub, err := sconn.Subscribe(bconf.SourceQueue, stomp.AckClient)
	if err != nil {
		log.Fatal(err)
	}
}
