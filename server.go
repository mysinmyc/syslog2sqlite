package syslog2sqlite

import (
	"github.com/ekanite/ekanite/input"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mysinmyc/gocommons/db"
	"github.com/mysinmyc/gocommons/diagnostic"
	"time"
)

type Server struct {
	collector    input.Collector
	dbHelper     *db.DbHelper
	sqlInsert    *db.SqlInsert
	eventChannel chan *input.Event
}

const (
	backLog           = 1000
	TABLE_SYSLOG      = "syslog"
	FIELD_TIMESTAMP   = "timestamp"
	FIELD_SOURCE_IP   = "source_ip"
	FIELD_MESSAGE_RAW = "message_raw"
	FIELD_PRIORITY    = "priority"
	FIELD_APP         = "app"
	FIELD_PID         = "pid"
	FIELD_MESSAGE     = "message"
	DDL_SYSLOG_SQLITE = "create table if not exists " + TABLE_SYSLOG + " (" +
		FIELD_TIMESTAMP + " timestamp " +
		", " + FIELD_SOURCE_IP + " text" +
		", " + FIELD_MESSAGE_RAW + " text" +
		", " + FIELD_PRIORITY + " integer" +
		", " + FIELD_APP + " text" +
		", " + FIELD_PID + " integer" +
		", " + FIELD_MESSAGE + " text" +
		")"
)

func NewServer(pCollector input.Collector, pDbPath string) (*Server, error) {

	vEventChannel := make(chan *input.Event, backLog)

	vDbHelper, vDbHelperError := db.NewDbHelper("sqlite3", pDbPath)
	if vDbHelperError != nil {
		return nil, diagnostic.NewError("Error opening database", vDbHelperError)
	}

	if _, vError := vDbHelper.Exec(DDL_SYSLOG_SQLITE); vError != nil {
		return nil, diagnostic.NewError("Error creating table", vError)
	}

	vSqlInsert, vCreateInsertError := vDbHelper.CreateInsert(TABLE_SYSLOG, []string{FIELD_TIMESTAMP, FIELD_SOURCE_IP, FIELD_MESSAGE_RAW, FIELD_PRIORITY, FIELD_APP, FIELD_PID, FIELD_MESSAGE}, db.InsertOptions{})
	if vCreateInsertError != nil {
		return nil, diagnostic.NewError("Error creating sqlinsert database", vCreateInsertError)
	}

	if _, vBeginBulkError := vSqlInsert.BeginBulk(db.BulkOptions{BatchSize:1000}); vBeginBulkError != nil {
		return nil, diagnostic.NewError("BeginBulkFailed", vBeginBulkError)
	}

	if vError := pCollector.Start(vEventChannel); vError != nil {
		return nil, diagnostic.NewError("Error starting collector", vError)
	}

	vRis := &Server{
		collector:    pCollector,
		dbHelper:     vDbHelper,
		sqlInsert:    vSqlInsert,
		eventChannel: vEventChannel}

	go vRis.eventDispatcherLoop()
	go vRis.committerLoop()

	return vRis, nil
}

func extractFieldsFromEvent(pEvent *input.Event) []interface{} {
	vRis := make([]interface{}, 0, 10)
	vRis = append(vRis, pEvent.ReferenceTime(), pEvent.SourceIP, pEvent.Text, pEvent.Parsed["priority"], pEvent.Parsed["app"], pEvent.Parsed["pid"], pEvent.Parsed["message"])
	return vRis
}

func (vSelf *Server) eventDispatcherLoop() {

	for vCurEvent := range vSelf.eventChannel{
		if _, vError := vSelf.sqlInsert.Exec(extractFieldsFromEvent(vCurEvent)...); vError != nil {
			diagnostic.LogError("Server.eventDispatcherLoop", "Failed to insert event %s", vError, vCurEvent)
		}
	}
}

func (vSelf *Server) committerLoop() {

	for {
	 	<- time.After(time.Duration(1) * time.Second) 
		//diagnostic.LogInfo("Server.eventDispatcherLoop", "Tick")
		if vError := vSelf.sqlInsert.Commit(); vError != nil {
			diagnostic.LogError("Server.eventDispatcherLoop", "Failed to commit", vError)
		}
	}
}

func (vSelf *Server) DbHelper() (*db.DbHelper) {
	return vSelf.dbHelper
}
