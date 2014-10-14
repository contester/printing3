package printserver

import (
	"log"

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

	return s.conn.Send(s.server.Destination, "application/octet-stream", contents, stomp.NewHeader("delivery-mode", "2"))
}

func (s *Server) Process(process func(*ServerConn, *stomp.Message) error) error {
	conn, err := s.StompConfig.NewConnection()
	//defer conn.Disconnect()

	if err != nil {
		return err
	}

	sub, err := conn.Subscribe(s.Source, stomp.AckClient)
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
			log.Printf("Started transaction: %+v\n", tx)
			err = process(&ServerConn{server: s, conn: tx}, msg)
			if err != nil {
				log.Printf("Processing error %s, aborting\n", err)
				tx.Abort()
			} else {
				log.Printf("Acking\n")
				if err = tx.Ack(msg); err != nil {
					log.Printf("Ack error: %s\n", err)
					conn.Disconnect()
					return err
				}
				log.Printf("Committing\n")
				err = tx.Commit()
			}
			log.Printf("Finished.\n")
		} else {
			err = process(&ServerConn{server: s, conn: conn}, msg)
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
