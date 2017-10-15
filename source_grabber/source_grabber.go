package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/golang/protobuf/proto"

	"encoding/json"
	"github.com/contester/printing3/grabber"
	"github.com/contester/printing3/printserver"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/stomp.v2"
)

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
Participants.LocalID = PrintJobs.Team and PrintJobs.ID = ?
`

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

func scanJob(r grabber.RowOrRows) (*scannedJob, error) {
	var job scannedJob
	if err := r.Scan(&job.JobID, &job.Filename, &job.ContestID, &job.ContestName, &job.TeamID, &job.SchoolName,
		&job.TeamNum, &job.ComputerID, &job.ComputerName, &job.AreaID, &job.AreaName, &job.Printer,
		&job.Data, &job.Arrived); err != nil {
		return nil, err
	}
	return &job, nil
}

func processJob(db *sqlx.DB, sender func(msg proto.Message) error, rows grabber.RowOrRows) error {
	job, err := scanJob(rows)
	if err != nil {
		return err
	}

	result := tickets.PrintJob{
		JobId:     proto.Uint32(uint32(job.JobID)),
		Filename:  &job.Filename,
		Printer:   &job.Printer,
		Computer:  &tickets.Computer{Id: &job.ComputerID, Name: &job.ComputerName},
		Contest:   &tickets.IdName{Id: proto.Uint32(uint32(job.ContestID)), Name: &job.ContestName},
		Area:      &tickets.IdName{Id: proto.Uint32(uint32(job.AreaID)), Name: &job.AreaName},
		Charset:   proto.String("windows-1251"),
		Team:      &tickets.IdName{Id: proto.Uint32(uint32(job.TeamID)), Name: &job.SchoolName},
		Timestamp: proto.Uint64(uint64(job.Arrived.UnixNano()) / 1000),
	}

	if job.TeamNum.Valid && job.TeamNum.Int64 > 0 {
		result.Team.Name = proto.String(result.Team.GetName() + " #" + strconv.FormatInt(job.TeamNum.Int64, 10))
	}

	result.Data, err = tickets.NewBlob(job.Data)
	if err != nil {
		return err
	}

	if err = sender(&result); err != nil {
		return err
	}

	if _, err = db.Exec("Update PrintJobs set Printed = 255 where ID = ?", job.JobID); err != nil {
		return err
	}
	fmt.Printf("Printed job %d\n", job.JobID)
	return nil
}

type printRequest struct {
	Id int32 `json:"id"`
}

type grserver struct {
	db *sqlx.DB
}

func (g grserver) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var ticket printRequest
	if err := json.Unmarshal(msg.Body, &ticket); err != nil {
		log.Printf("Received malformed job: %s", err)
		return err
	}

	rows, err := g.db.Query(PRINT_QUERY, ticket.Id)
	if err != nil {
		log.Printf("Error looking up submit: %s", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = processJob(g.db, conn.Send, rows); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()

	config, err := tools.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	var gs grserver
	gs.db, err = tools.CreateDb(config.Server.Db)
	if err != nil {
		log.Fatal(err)
	}

	pserver := printserver.Server{
		Source:      "/amq/queue/contester.printrequests",
		Destination: "/amq/queue/source_pb",
		StompConfig: &config.Messaging,
	}

	for {
		log.Println(pserver.Process(gs.processIncoming))
		time.Sleep(15 * time.Second)
	}
}
