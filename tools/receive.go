package tools

import (
	"context"

	"github.com/go-stomp/stomp"
	"google.golang.org/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

func MaybeAck(msg *stomp.Message) error {
	if msg.ShouldAck() {
		return msg.Conn.Ack(msg)
	}
	return nil
}

// Outgoing is a message for SendAllAndAck.
type Outgoing struct {
	Dest string
	Msg  proto.Message
}

// SendAndAck sends one message and acks the incoming one.
func SendAndAck(msg *stomp.Message, dest string, data proto.Message) error {
	return SendAllAndAck(msg, Outgoing{dest, data})
}

// SendAllAndAck sends the messages in the given order and acks the incoming
// one, all in one transaction when the incoming message needs an ack.
func SendAllAndAck(msg *stomp.Message, outs ...Outgoing) error {
	bufs := make([][]byte, len(outs))
	for i, o := range outs {
		buf, err := proto.Marshal(o.Msg)
		if err != nil {
			log.Errorf("error marshaling message %v: %v", o.Msg, err)
			return MaybeAck(msg)
		}
		bufs[i] = buf
	}
	if msg.ShouldAck() {
		tx := msg.Conn.Begin()
		for i, o := range outs {
			if err := tx.Send(o.Dest, "application/vnd.google.protobuf", bufs[i], stomp.SendOpt.Header("delivery-mode", "2")); err != nil {
				log.Errorf("error sending message %v in transaction: %v", o.Msg, err)
				return err
			}
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
	for i, o := range outs {
		if err := msg.Conn.Send(o.Dest, "application/vnd.google.protobuf", bufs[i], stomp.SendOpt.Header("delivery-mode", "2")); err != nil {
			return err
		}
	}
	return nil
}

func SubscribeAndProcess(ctx context.Context, conn *stomp.Conn, queue string, proc func(context.Context, *stomp.Message) error) (*stomp.Subscription, error) {
	sub, err := conn.Subscribe(queue, stomp.AckClient)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case v, ok := <-sub.C:
				if !ok {
					log.Fatalf("subscription %q closed", sub.Id())
				}
				if err := proc(ctx, v); err != nil {
					log.Fatalf("process error: %v", err)
				}
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			}
		}
	}()
	return sub, nil
}
