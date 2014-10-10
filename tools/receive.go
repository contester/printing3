package tools

import (
	l4g "code.google.com/p/log4go"
	"github.com/jjeffery/stomp"
	"time"
)

type Conn interface {

}

func (pc *StompConfig) ReceiveLoop(queueName string, process func(*stomp.Conn, *stomp.Message) error) error {
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
		err = process(conn, msg)
		if err != nil {
			l4g.Error("Processing failed with error: %s, nacking", err)
			err = conn.Nack(msg)
			time.Sleep(15 * time.Second)
		} else {
			err = conn.Ack(msg)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
