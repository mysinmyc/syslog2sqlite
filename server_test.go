package syslog2sqlite

import (
	"github.com/ekanite/ekanite/input"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
	"log"
)

const (
	executors = 16
	messages  = 500
)

func sendData(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	for vCnt := 0; vCnt < messages; vCnt++ {
		if _, vError:=conn.Write([]byte("<134>0 2017-06-04T14:09:13+02:00 192.168.1.217 filterlog - - 67,,,0,vtnet0,match,pass,out,4,0x0,,127,3328,0,DF,6,tcp,366,192.168.1.66,31.13.86.4,50800,443,326,PA,1912507082:1912507408,2077294259,257,,\n")) ; vError!=nil {
			log.Printf("Error: %s",vError.Error())	
		}
	}
	return nil
}

func Test_Collector(t *testing.T) {

	var waitGroup sync.WaitGroup
	collector, err := input.NewCollector("tcp", "127.0.0.1:0", "syslog", nil)
	if err != nil {
		t.Fatalf("failed to create test collector: %s", err.Error())
	}

	vTempDb := os.TempDir() + "/__test" + strconv.Itoa(os.Getpid()) + ".db"
	defer os.Remove(vTempDb)
	vServer, vServerError := NewServer(collector, vTempDb)
	if vServerError != nil {
		t.Fatalf("failed to create server: %s", vServerError)
	}

	for vCnt := 0; vCnt < executors; vCnt++ {
		waitGroup.Add(1)
		go func() {
			sendError := sendData(collector.Addr().String())
			waitGroup.Done()
			if sendError != nil {
				t.Fatalf("Error sending data to collector: %s", sendError.Error())
			}
		}()
	}

	waitGroup.Wait()
	time.Sleep(time.Millisecond * 5000)
	
	vRow:=vServer.DbHelper().GetDb().QueryRow("select count(*) from syslog")

	var vRowsInTable int
	vCountError:=vRow.Scan(&vRowsInTable) 
	if vCountError != nil {
		t.Error("an error occurred while counting rows")
	}	

	vExpected:= executors*messages
	if vRowsInTable !=vExpected  {
		t.Errorf("invalid number of rows in table: current %d expected %d",vRowsInTable,vExpected)
	}
}
