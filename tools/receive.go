package tools

import (
	"context"

	"github.com/go-stomp/stomp"
	"github.com/gogo/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

func MaybeAck(msg *stomp.Message) error {
	if msg.ShouldAck() {
		return msg.Conn.Ack(msg)
	}
	return nil
}

func SendAndAck(msg *stomp.Message, dest string, data proto.Message) error {
	buf, err := proto.Marshal(data)
	if err != nil {
		log.Errorf("error marshaling message %v: %v", data, err)
		return MaybeAck(msg)
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

func SubscribeAndProcess(ctx context.Context, conn *stomp.Conn, queue string, proc func(context.Context, *stomp.Message) error) (*stomp.Subscription, error) {
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
