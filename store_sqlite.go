package syslog2sqlite

import (
	"github.com/ekanite/ekanite/input"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mysinmyc/gocommons/db"
	"github.com/mysinmyc/gocommons/diagnostic"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
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
	commitIntervalSeconds       = time.Duration(5) * time.Second
	archiveCheckIntervalSeconds = time.Duration(60) * time.Second
)

type EventStoreSqlLiteOptions struct {
	MaxFileSizeBytes int64
	MaxAge           time.Duration
	ArchiveDir       string
}

type EventStoreSqlLite struct {
	dbPath      string
	dbHelper    *db.DbHelper
	sqlInsert   *db.SqlInsert
	options     EventStoreSqlLiteOptions
	stopChannel chan bool
	mutex       *sync.Mutex
	startTime   time.Time
}

func NewEventStoreSqlLite(pDbPath string, pOptions EventStoreSqlLiteOptions) (*EventStoreSqlLite, error) {

	vRis := &EventStoreSqlLite{
		dbPath:  pDbPath,
		options: pOptions,
		mutex:   &sync.Mutex{}}

	if vError := vRis.initDb(); vError != nil {
		return nil, diagnostic.NewError("Failed to initialize db", vError)
	}
	return vRis, nil
}

func (vSelf *EventStoreSqlLite) initDb() error {

	vSelf.startTime = time.Time{}
	vDbHelper, vDbHelperError := db.NewDbHelper("sqlite3", vSelf.dbPath)
	if vDbHelperError != nil {
		return diagnostic.NewError("Error opening database", vDbHelperError)
	}

	vSelf.dbHelper = vDbHelper
	if _, vError := vDbHelper.Exec(DDL_SYSLOG_SQLITE); vError != nil {
		return diagnostic.NewError("Error creating table", vError)
	}

	vSqlInsert, vCreateInsertError := vDbHelper.CreateInsert(TABLE_SYSLOG, []string{FIELD_TIMESTAMP, FIELD_SOURCE_IP, FIELD_MESSAGE_RAW, FIELD_PRIORITY, FIELD_APP, FIELD_PID, FIELD_MESSAGE}, db.InsertOptions{})
	if vCreateInsertError != nil {
		return diagnostic.NewError("Error creating sqlinsert", vCreateInsertError)
	}
	vSelf.sqlInsert = vSqlInsert

	if _, vBeginBulkError := vSqlInsert.BeginBulk(db.BulkOptions{BatchSize: 1000}); vBeginBulkError != nil {
		return diagnostic.NewError("BeginBulkFailed", vBeginBulkError)
	}

	return nil
}

func (vSelf *EventStoreSqlLite) GetStartTime() (time.Time, error) {

	if vSelf.startTime.IsZero() == false {
		return vSelf.startTime, nil
	}

	vTimestampResultSet, vTimestampResultSetError := vSelf.dbHelper.Query("select min("+FIELD_TIMESTAMP+") from " + TABLE_SYSLOG)
	if vTimestampResultSetError != nil {
		return time.Time{}, diagnostic.NewError("Error executing StartTime query", vTimestampResultSetError)
	}
	defer vTimestampResultSet.Close()

	if vTimestampResultSet.Next() == false {
		diagnostic.LogTrace("EventStoreSqlLite.GetStartTime", "EMPTY DB")
		return time.Now(), nil
	}

	var vMinTimeRaw []byte
	if vError := vTimestampResultSet.Scan(&vMinTimeRaw); vError != nil {
		return time.Now(), diagnostic.NewError("Error parsing StartTime", vError)
	}

	if db.SqlLiteTimestamp(vMinTimeRaw).IsNull() {
		diagnostic.LogTrace("EventStoreSqlLite.GetStartTime", "EMPTY MIN TIME")
		return time.Now(), nil
	}

	vMinTime,vMinTimeError:=db.SqlLiteTimestamp(vMinTimeRaw).Time()
	if vMinTimeError != nil {
		return time.Now(), diagnostic.NewError("Error parsing StartTimeRaw", vMinTimeError)
	}	

	diagnostic.LogDebug("EventStoreSqlLite.GetStartTime", "Loaded startime from db: %v", vMinTime)
	vSelf.startTime=vMinTime
	
	return vSelf.startTime, nil
}



func (vSelf *EventStoreSqlLite) Start(pChannel chan *input.Event) error {
	diagnostic.LogInfo("EventStoreSqlLite.Start", "Starting eventstore into %s...", vSelf.dbPath)
	vSelf.stopChannel = make(chan bool)
	go vSelf.eventDispatcherLoop(pChannel)
	go vSelf.committerLoop()
	go vSelf.archiveLoop()
	return nil
}

func extractFieldsFromEvent(pEvent *input.Event) []interface{} {
	vRis := make([]interface{}, 0, 10)
	vRis = append(vRis, pEvent.ReferenceTime(), pEvent.SourceIP, pEvent.Text, pEvent.Parsed["priority"], pEvent.Parsed["app"], pEvent.Parsed["pid"], pEvent.Parsed["message"])
	return vRis
}

func (vSelf *EventStoreSqlLite) eventDispatcherLoop(pChannel chan *input.Event) {

	for {
		select {

		case <-vSelf.stopChannel:
			return

		case vCurEvent := <-pChannel:
			if vCurEvent == nil {
				return
			}
			vSelf.mutex.Lock()
			if _, vError := vSelf.sqlInsert.Exec(extractFieldsFromEvent(vCurEvent)...); vError != nil {
				diagnostic.LogError("Server.eventDispatcherLoop", "Failed to insert event %s", vError, vCurEvent)
			}
			vSelf.mutex.Unlock()
		}
	}
}

func (vSelf *EventStoreSqlLite) committerLoop() {
	vTimeoutChannel := time.After(commitIntervalSeconds)
	for {
		select {
		case <-vSelf.stopChannel:
			return
		case <-vTimeoutChannel:
			if vError := vSelf.sqlInsert.Commit(); vError != nil {
				diagnostic.LogError("Server.eventDispatcherLoop", "Failed to commit", vError)
			}
			vTimeoutChannel = time.After(commitIntervalSeconds)
		}
	}
}

func (vSelf *EventStoreSqlLite) archiveLoop() {
	vTimeoutChannel := time.After(archiveCheckIntervalSeconds)
	for {
		select {
		case <-vSelf.stopChannel:
			return
		case <-vTimeoutChannel:

			vArchiveTresholdsReached, vArchiveTresholdsReachedError := vSelf.archiveTresholdsReached()
			if vArchiveTresholdsReachedError != nil {
				diagnostic.LogError("EventStoreSqlLite.archiveLoop", "error testing archive", vArchiveTresholdsReachedError)
			}

			if vArchiveTresholdsReached {
				if vPerfomArchiveError := vSelf.Archive(); vPerfomArchiveError != nil {
					diagnostic.LogError("EventStoreSqlLite.archiveLoop", "an error occurred during archive", vPerfomArchiveError)

				}
			}
			vTimeoutChannel = time.After(archiveCheckIntervalSeconds)
		}
	}
}

func (vSelf *EventStoreSqlLite) DbHelper() *db.DbHelper {
	return vSelf.dbHelper
}

func (vSelf *EventStoreSqlLite) Stop() error {
	diagnostic.LogInfo("EventStoreSqlLite.Stop", "Stopping eventstore %s...", vSelf.dbPath)
	close(vSelf.stopChannel)
	if vError := vSelf.sqlInsert.Commit(); vError != nil {
		return diagnostic.NewError("An error occurred while performing final commit", vError)
	}
	return nil

}

func getArchiveFile(pSourceFile, pArchiveDir string) string {

	vTargetDir := pArchiveDir
	if vTargetDir == "" {
		vTargetDir = path.Dir(pSourceFile)
	}

	vExtension := path.Ext(pSourceFile)
	vFileName := strings.TrimSuffix(path.Base(pSourceFile), vExtension) + "_" + time.Now().Format("2006-01-02_150405") + vExtension
	return vTargetDir + "/" + vFileName

}

func (vSelf *EventStoreSqlLite) Archive() error {
	vTargetFile := getArchiveFile(vSelf.dbPath, vSelf.options.ArchiveDir)

	diagnostic.LogInfo("EventStoreSqlLite.Archive", "Requested to archive %s into %s", vSelf.dbPath, vTargetFile)
	vSelf.mutex.Lock()
	defer vSelf.mutex.Unlock()

	diagnostic.LogDebug("EventStoreSqlLite.Archive", "closing db...")
	if vError := vSelf.sqlInsert.Close(); vError != nil {
		return diagnostic.NewError("An error occurred while terminating db insert", vError)
	}

	if vError := vSelf.dbHelper.Close(); vError != nil {
		return diagnostic.NewError("An error occurred while closing db helper", vError)
	}

	diagnostic.LogDebug("EventStoreSqlLite.Archive", "moving old file...")
	if vError := os.Rename(vSelf.dbPath, vTargetFile); vError != nil {
		return diagnostic.NewError("An error occurred while moving file", vError)
	}

	diagnostic.LogDebug("EventStoreSqlLite.Archive", "initializing db...")
	if vError := vSelf.initDb(); vError != nil {
		return diagnostic.NewError("An error occurred while moving file", vError)
	}

	diagnostic.LogInfo("EventStoreSqlLite.Archive", "Archive succeded")
	return nil
}

func (vSelf *EventStoreSqlLite) archiveTresholdsReached() (bool, error) {
	vFileInfo, vFileInfoError := os.Stat(vSelf.dbPath)
	if vFileInfoError != nil {
		return false, diagnostic.NewError("Error stats file %s", vFileInfoError, vSelf.dbPath)
	}

	if vSelf.options.MaxFileSizeBytes > 0 && vFileInfo.Size() > vSelf.options.MaxFileSizeBytes {
		return true, nil
	}

	if vSelf.options.MaxAge > 0 && vSelf.startTime.Add(vSelf.options.MaxAge).After(time.Now()) == false {
		return true, nil
	}

	return false, nil
}
