package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/jmoiron/sqlx"
	"gopkg.in/stomp.v1"

	_ "github.com/go-sql-driver/mysql"
)

func createDb(spec string) (db *sqlx.DB, err error) {
	if db, err = sqlx.Connect("mysql", spec+"?parseTime=true&charset=utf8mb4,utf8"); err != nil {
		return
	}
	return
}

const PRINT_QUERY = `select
PrintJobs.ID as ID, Filename, PrintJobs.Contest as ContestID,
Contests.Name as ContestName, PrintJobs.Team as TeamID,
Schools.Name as SchoolName, Teams.Num as TeamNum,
inet_ntoa(PrintJobs.Computer) as ComputerID,
CompLocations.Name as ComputerName,
Areas.ID as AreaID, Areas.Name as AreaName, Printer, Data, Arrived
from PrintJobs, Contests, Areas, Participants, Teams, Schools, CompLocations
where
Contests.ID = Participants.Contest and Contests.ID = PrintJobs.Contest and CompLocations.ID = PrintJobs.Computer and
Teams.ID = Participants.Team and Teams.School = Schools.ID and Areas.ID = CompLocations.Location and
Participants.LocalID = PrintJobs.Team and Printed is null
`

type printProcessor struct {
	*tools.StompConfig
	db        *sqlx.DB
	conn      *stomp.Conn
	queueName string
}

type scannedJob struct {
	JobID                    int64
	Filename                 string
	ContestID                int64
	ContestName              string
	TeamID                   int64
	SchoolName               string
	TeamNum                  sql.NullInt64
	ComputerID, ComputerName string
	AreaID                   int64
	AreaName                 string
	Printer                  string
	Data                     []byte
	Arrived                  time.Time
}

type rowOrRows interface {
	Scan(dest ...interface{}) error
}

func scanJob(r rowOrRows) (*scannedJob, error) {
	var job scannedJob
	if err := r.Scan(&job.JobID, &job.Filename, &job.ContestID, &job.ContestName, &job.TeamID, &job.SchoolName,
		&job.TeamNum, &job.ComputerID, &job.ComputerName, &job.AreaID, &job.AreaName, &job.Printer,
		&job.Data, &job.Arrived); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *printProcessor) processJob(job *scannedJob) {
	result := tickets.PrintJob{
		JobId:     proto.Uint32(uint32(job.JobID)),
		Filename:  &job.Filename,
		Printer:   &job.Printer,
		Computer:  &tickets.Computer{Id: &job.ComputerID, Name: &job.ComputerName},
		Contest:   &tickets.IdName{Id: proto.Uint32(uint32(job.ContestID)), Name: &job.ContestName},
		Area:      &tickets.IdName{Id: proto.Uint32(uint32(job.AreaID)), Name: &job.AreaName},
		Charset:   proto.String("cp1251"),
		Team:      &tickets.IdName{Id: proto.Uint32(uint32(job.TeamID)), Name: &job.SchoolName},
		Timestamp: proto.Uint64(uint64(job.Arrived.UnixNano()) / 1000),
	}

	if job.TeamNum.Valid && job.TeamNum.Int64 > 0 {
		result.Team.Name = proto.String(result.Team.GetName() + " #" + strconv.FormatInt(job.TeamNum.Int64, 10))
	}

	body, err := proto.Marshal(&result)
	if err != nil {
		return
	}

	for {
		if s.conn == nil {
			s.conn, err = s.StompConfig.NewConnection()
			if err != nil {
				log.Printf("(retry in 15s) Error connecting to stomp: %s", err)
				time.Sleep(15 * time.Second)
				continue
			}
		}

		err = s.conn.SendWithReceipt("/amq/queue/source_pb", "application/octet-stream", body, stomp.NewHeader("delivery-mode", "2"))
		if err == nil {
			break
		}
		s.conn.Disconnect()
		s.conn = nil
	}
	s.db.Exec("Update PrintJobs set Printed = 255 where ID = ?", job.JobID)
	fmt.Printf("Printed job %d\n", job.JobID)
}

func (s *printProcessor) scan() {
	rows, err := s.db.Query(PRINT_QUERY)
	if err != nil {
		log4go.Error("Error in submit: %s", err)
		return
	}
	for rows.Next() {
		if job, err := scanJob(rows); job != nil {
			s.processJob(job)
		} else {
			log4go.Error("Error reading submit info: %s", err)
		}
	}
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	dbSpec := flag.String("db", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	config, err := tools.MaybeReadConfigFile(*configFileName)

	if config != nil {
		if s, err := config.GetString("server", "db"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			*dbSpec = s
		}
	}

	var srv printProcessor

	srv.db, err = createDb(*dbSpec)
	if err != nil {
		log4go.Error("Opening db connection to %s: %s", *dbSpec, err)
		return
	}

	srv.StompConfig, err = tools.ParseStompFlagOrConfig(*stompSpec, config, "messaging")
	if err != nil {
		log4go.Error("Opening stomp connection to %s: %s", *stompSpec, err)
		return
	}

	for {
		srv.scan()
		log.Printf("Scan complete, sleeping for 15s")
		time.Sleep(15 * time.Second)
	}
}
