package tools

import (
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
)

type Conn interface {
	Send(destination, contentType string, body []byte, opts ...func(*frame.Frame) error) error
}

func (pc *StompConfig) ReceiveLoop(queueName string, useTransactions bool, process func(Conn, *stomp.Message) error) error {
	conn, err := pc.NewConnection()
	//defer conn.Disconnect()

	if err != nil {
		return err
	}

	sub, err := conn.Subscribe(queueName, stomp.AckClientIndividual)
	if err != nil {
		return err
	}

	for {
		msg, err := sub.Read()
		if err != nil {
			return err
		}
		if useTransactions {
			tx := conn.Begin()
			err = process(tx, msg)
			if err != nil {
				tx.Abort()
			} else {
				tx.Ack(msg)
				err = tx.Commit()
			}
		} else {
			err = process(conn, msg)
			if err != nil {
				err = conn.Nack(msg)
			} else {
				err = conn.Ack(msg)
			}
		}
		if err != nil {
			return err
		}
	}
}
