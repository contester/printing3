package printserver

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/contester/printing3/tools"
	"gopkg.in/stomp.v1"
)

type Server struct {
	StompConfig         *tools.StompConfig
	Source, Destination string
}

type ServerConn struct {
	server *Server
	conn   tools.Conn
}

func (s *ServerConn) Send(msg proto.Message) error {
	contents, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return s.conn.SendWithReceipt(s.server.Destination, "application/binary", contents, stomp.NewHeader("delivery-mode", "2"))
}

func (s *Server) Process(process func(*ServerConn, *stomp.Message) error) error {
	conn, err := s.StompConfig.NewConnection()
	//defer conn.Disconnect()

	if err != nil {
		return err
	}

	sub, err := conn.Subscribe(s.Source, stomp.AckClientIndividual)
	if err != nil {
		return err
	}

	for {
		msg, err := sub.Read()
		if err != nil {
			return err
		}
		if s.Destination != "" {
			tx := conn.Begin()
			err = process(&ServerConn{server: s, conn: tx}, msg)
			if err != nil {
				tx.Abort()
			} else {
				tx.Ack(msg)
				err = tx.Commit()
			}
		} else {
			err = process(nil, msg)
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
	return nil

}
