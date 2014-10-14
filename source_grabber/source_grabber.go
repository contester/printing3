package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"

	"github.com/contester/printing3/grabber"
	_ "github.com/go-sql-driver/mysql"
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
Participants.LocalID = PrintJobs.Team and Printed is null
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

func processJob(g *grabber.Grabber, rows grabber.RowOrRows) error {
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

	if err = g.Send(&result); err != nil {
		return err
	}

	if _, err = g.DB.Exec("Update PrintJobs set Printed = 255 where ID = ?", job.JobID); err != nil {
		return err
	}
	fmt.Printf("Printed job %d\n", job.JobID)
	return nil
}

func main() {
	flag.Parse()

	config, err := tools.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	dbSpec, err := config.GetString("server", "db")
	if err != nil {
		log.Fatal(err)
	}

	g, err := grabber.New(dbSpec, PRINT_QUERY, "/amq/queue/source_pb")
	if err != nil {
		log.Printf("Opening db connection to %+v: %s", dbSpec, err)
		return
	}

	g.StompConfig, err = tools.ParseStompFlagOrConfig("", config, "messaging")
	if err != nil {
		log.Fatalf("Opening stomp connection: %s", err)
		return
	}

	for {
		g.Scan(processJob)
		log.Printf("Scan complete, sleeping for 15s")
		time.Sleep(15 * time.Second)
	}
}
