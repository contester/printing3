package grabber

import (
	"time"

	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"

	log "github.com/sirupsen/logrus"
)

type Grabber struct {
	StompConfig        *tools.StompConfig
	DB                 *sqlx.DB
	Conn               *stomp.Conn
	Query, Destination string
}

type RowOrRows interface {
	Scan(dest ...interface{}) error
}

func createDb(spec string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("mysql", spec+"?parseTime=true&charset=utf8mb4,utf8")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func New(dbSpec, query, destination string) (*Grabber, error) {
	result := Grabber{
		Query:       query,
		Destination: destination,
	}
	var err error
	result.DB, err = createDb(dbSpec)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Grabber) Scan(process func(*Grabber, RowOrRows) error) error {
	rows, err := s.DB.Query(s.Query)
	if err != nil {
		log.Printf("Error in submit: %s", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = process(s, rows); err != nil {
			return err
		}
	}
	return nil
}

func (s *Grabber) Send(msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling %+v: %s", msg, err)
		return err
	}

	for {
		if s.Conn == nil {
			s.Conn, err = s.StompConfig.NewConnection()
			if err != nil {
				log.Printf("(retry in 15s) Error connecting to stomp: %s", err)
				time.Sleep(15 * time.Second)
				continue
			}
		}

		if err = s.Conn.Send(s.Destination, "application/octet-stream", body,
			stomp.SendOpt.Receipt,
			stomp.SendOpt.Header("delivery-mode", "2")); err == nil {
			break
		}
		s.Conn.Disconnect()
		s.Conn = nil
	}
	return nil
}
