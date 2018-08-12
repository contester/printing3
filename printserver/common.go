package printserver

import (
	"log"

	"github.com/contester/printing3/tools"
	"github.com/golang/protobuf/proto"
	"gopkg.in/stomp.v2"
	"gopkg.in/stomp.v2/frame"
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
	log.Printf("send: %s", msg)
	contents, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.SendContents(contents, "")
}

func (s *ServerConn) SendContents(contents []byte, contentType string) error {
	if s.server.Destination == "" {
		return nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return s.conn.Send(s.server.Destination, contentType, contents,
		stomp.SendOpt.Header(frame.NewHeader("delivery-mode", "2")))
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
