package main

import (
	"flag"
	"github.com/mysinmyc/gocommons/diagnostic"
	"github.com/ekanite/ekanite/input"
	"github.com/mysinmyc/syslog2sqlite"
	"time"
	"os"
	"syscall"
	"os/signal"
)

var (
	flag_Listen                = flag.String("listen", "0.0.0.0:8080", "Listening address")
	flag_Debug                 = flag.Bool("debug", false, "Enable debug")
	flag_Db_Path                = flag.String("db_path", "", "DB PATH")
	flag_Archive_Dir           = flag.String("archive_dir", "", "Directory where to move archived db")
	flag_Archive_MaxSizeMB     = flag.Int64("archive_maxSizeMB", 100, "Maximum file size in MegaBytes")
	flag_Archive_MaxAgeMinutes = flag.Int64("archive_maxAgeMinutes", 1440, "Maximum db age in Minutes")
)

// waitForSignals blocks until a signal is received.
func waitForSignals() {
        // Set up signal handling.
        signalCh := make(chan os.Signal, 1)
        signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

        // Block until one of the signals above is received
        select {
        case <-signalCh:
                diagnostic.LogInfo("main.waitForSignals","signal received, shutting down...")
        }
}

func main() {

	flag.Parse()

	if *flag_Debug {
		diagnostic.SetLogLevel(diagnostic.LogLevel_Debug)
	} else {
		diagnostic.SetLogLevel(diagnostic.LogLevel_Info)
	}

	if *flag_Db_Path == "" {
		flag.Usage()	
		diagnostic.LogFatal("main", "Missing parameter db_path", nil)
	}



	diagnostic.LogInfo("main", "Initialize collector listening on %s...", *flag_Listen)
	vCollector, vError := input.NewCollector("tcp", *flag_Listen, "syslog", nil)
	diagnostic.LogFatalIfError(vError,"main", "failed to initialize collector")



	diagnostic.LogInfo("main", "Initialize EventStore to db  %s...", *flag_Db_Path)
	vEventStore, vError := syslog2sqlite.NewEventStoreSqlLite(*flag_Db_Path,
		syslog2sqlite.EventStoreSqlLiteOptions{
			MaxFileSizeBytes: *flag_Archive_MaxSizeMB * 1024 * 1024,
			MaxAge:           time.Duration(*flag_Archive_MaxAgeMinutes) * time.Minute,
			ArchiveDir:       *flag_Archive_Dir})
	diagnostic.LogFatalIfError(vError,"main", "failed to initialize event store")
	


	diagnostic.LogInfo("main","Initialize Server...")
	vServer, vError := syslog2sqlite.NewServer(vCollector, vEventStore)
	diagnostic.LogFatalIfError(vError,"main", "failed to initialize collector")



	diagnostic.LogInfo("main","Start server...")
	vError = vServer.Start()
	diagnostic.LogFatalIfError(vError,"main", "failed to start server")

	diagnostic.LogInfo("main","Server started")
	waitForSignals()
	
	diagnostic.LogInfo("main","Requested shutdown")
}
