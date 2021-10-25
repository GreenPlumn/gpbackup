package helper

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Backup specific functions
 */

// timeStart := time.Now()
// execTime += time.Since(execStart)
// func timeTrack(start time.Time, name string) {
// 	elapsed := time.Since(start)
// 	log.Printf("%s took %s\n", name, elapsed)
// }

// defer timeTrack(time.Now(), "backupDataForAllTables")

var loopFrontTime time.Duration
var loopBackTime time.Duration
var ioCopyTime time.Duration

var aTime time.Duration
var bTime time.Duration
var channelBlockTime time.Duration

func doBackupAgent() error {
	timeStart := time.Now()
	var lastRead uint64
	var (
		pipeWriter BackupPipeWriterCloser
		writeCmd   *exec.Cmd
	)
	tocfile := &toc.SegmentTOC{}
	tocfile.DataEntries = make(map[uint]toc.SegmentDataEntry)

	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}
	/*
	 * It is important that we create the reader before creating the writer
	 * so that we establish a connection to the first pipe (created by gpbackup)
	 * and properly clean it up if an error occurs while creating the writer.
	 */
	readerSlice := make([]io.Reader, len(oidList))
	readHandleSlice := make([]io.ReadCloser, len(oidList))
	channelSlice := make([]chan bool, len(oidList))
	for i, _ := range oidList {
		c := make(chan bool)
		channelSlice[i] = c
	}
	go func() {
		openPipeStart := time.Now()
		readHandle, err := os.OpenFile(fmt.Sprintf("%s_%d", *pipeFile, oidList[0]), os.O_RDONLY, os.ModeNamedPipe)
		atomic.AddUint64(&openPipeTime, uint64(time.Since(openPipeStart)/time.Microsecond))
		log(fmt.Sprintf("ADDING TIME=ASDF=%d=%#v===========================\n", openPipeTime, openPipeTime))

		if err != nil {
			panic(err)
		}
		reader := bufio.NewReader(readHandle)
		readerSlice[0] = reader
		readHandleSlice[0] = readHandle
		channelSlice[0] <- true

		for i, oid := range oidList[1:] {
			go func(i int, oid int) {
				// loopFrontStart := time.Now() //======================================================
				// aStart := time.Now()

				pipe := fmt.Sprintf("%s_%d", *pipeFile, oid)
				// log(fmt.Sprintf("Creating pipe %s\n", pipe))
				err := createPipe(pipe)
				if err != nil {
					// return err
					panic("2================")
				}

				openPipeStart := time.Now()
				readHandle, err := os.OpenFile(fmt.Sprintf("%s_%d", *pipeFile, oid), os.O_RDONLY, os.ModeNamedPipe)
				atomic.AddUint64(&openPipeTime, uint64(time.Since(openPipeStart)/time.Millisecond))

				if err != nil {
					panic(err)
				}
				reader := bufio.NewReader(readHandle)

				readerSlice[i+1] = reader
				readHandleSlice[i+1] = readHandle
				channelSlice[i+1] <- true

				// aTime += time.Since(aStart)
			}(i, oid)
		}
	}()

	pipeWriter, writeCmd, err = getBackupPipeWriter()
	if err != nil {
		return err
	}
	for i, oid := range oidList {

		if wasTerminated {
			return errors.New("Terminated due to user request")
		}
		// if i < len(oidList)-1 {
		// 	log(fmt.Sprintf("Creating pipe for oid %d\n", oidList[i+1]))
		// 	go func() {
		// 		nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
		// 		err := createPipe(nextPipe)
		// 		if err != nil {
		// 			return err
		// 		}
		// 	}()
		// }

		// bStart := time.Now()

		// bTime += time.Since(bStart)

		// log(fmt.Sprintf("Opening pipe for oid %d\n", oid))
		channelStart := time.Now()

		<-channelSlice[i] // block until pipe is open
		// block until next 5 pipes exist

		timeBlocked := time.Since(channelStart)
		log(fmt.Sprintf("time spent blocked on table %d: %v", i, timeBlocked))
		channelBlockTime += timeBlocked
		// log(fmt.Sprintf("ReadHandleSlice %v", readHandleSlice[i]))

		// log(fmt.Sprintf("Backing up table with oid %d\n", oid))

		// loopFrontTime += time.Since(loopFrontStart) // =================================================================================
		ioCopyStart := time.Now()

		numBytes, err := io.Copy(pipeWriter, readerSlice[i])
		if err != nil {
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}

		ioCopyTime += time.Since(ioCopyStart)
		loopBackStart := time.Now()

		// log(fmt.Sprintf("Read %d bytes\n", numBytes))

		lastProcessed := lastRead + uint64(numBytes)
		tocfile.AddSegmentDataEntry(uint(oid), lastRead, lastProcessed)
		lastRead = lastProcessed

		// lastPipe = currentPipe
		// currentPipe = nextPipe
		// err = utils.RemoveFileIfExists(lastPipe)
		// if err != nil {
		// 	return err
		// }
		_ = readHandleSlice[i].Close()
		err = utils.RemoveFileIfExists(fmt.Sprintf("%s_%d", *pipeFile, oidList[i]))
		if err != nil {
			return err
		}
		loopBackTime += time.Since(loopBackStart)
	}

	endStart := time.Now()
	_ = pipeWriter.Close()
	if *pluginConfigFile != "" {
		/*
		 * When using a plugin, the agent may take longer to finish than the
		 * main gpbackup process. We either write the TOC file if the agent finishes
		 * successfully or write an error file if it has an error after the COPYs have
		 * finished. We then wait on the gpbackup side until one of those files is
		 * written to verify the agent completed.
		 */
		log("Uploading remaining data to plugin destination")
		err := writeCmd.Wait()
		if err != nil {
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
	}
	err = tocfile.WriteToFileAndMakeReadOnly(*tocFile)
	if err != nil {
		return err
	}
	log("Finished writing segment TOC")
	helperEndTime := time.Since(endStart)

	log(fmt.Sprintf("loopFrontTime %v", loopFrontTime))
	// log(fmt.Sprintf("loop aTime %v", aTime))
	// log(fmt.Sprintf("loop bTime %v", bTime))
	log(fmt.Sprintf("loop openPipeTime %vms", openPipeTime))
	log(fmt.Sprintf("Average Pipe open Time: %vms", openPipeTime/uint64(len(oidList))))
	log(fmt.Sprintf("loop bbTime %v", bbTime))
	log(fmt.Sprintf("loop channelBlockTime %v", channelBlockTime))
	log(fmt.Sprintf("ioCopyTime %v", ioCopyTime))
	// log(fmt.Sprintf("loopBackTime %v", loopBackTime))
	log(fmt.Sprintf("helperEndTime %v", helperEndTime))
	log(fmt.Sprintf("doBackupagent %v", time.Since(timeStart)))

	return nil
}

var openPipeTime uint64
var bbTime time.Duration

// func getBackupPipeReader(currentPipe string) (io.Reader, io.ReadCloser, error) {
// 	openPipeStart := time.Now()

// 	readHandle, err := os.OpenFile(currentPipe, os.O_RDONLY, os.ModeNamedPipe)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	reader := bufio.NewReader(readHandle)

// 	openPipeTime = uint64(time.Since(openPipeStart) / time.Millisecond)
// 	atomic.AddUint64(&openPipeTime, openPipeTime)

// 	bbStart := time.Now()

// 	// This is a workaround for https://github.com/golang/go/issues/24164.
// 	// Once this bug is fixed, the call to Fd() can be removed
// 	// readHandle.Fd()
// 	reader := bufio.NewReader(readHandle)
// 	bbTime += time.Since(bbStart)
// 	return reader, readHandle, nil
// }

func getBackupPipeWriter() (pipe BackupPipeWriterCloser, writeCmd *exec.Cmd, err error) {
	var writeHandle io.WriteCloser
	if *pluginConfigFile != "" {
		writeCmd, writeHandle, err = startBackupPluginCommand()
	} else {
		writeHandle, err = os.Create(*dataFile)
	}
	if err != nil {
		return nil, nil, err
	}

	if *compressionLevel == 0 {
		pipe = NewCommonBackupPipeWriterCloser(writeHandle)
		return
	}

	if *compressionType == "gzip" {
		pipe, err = NewGZipBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}
	if *compressionType == "zstd" {
		pipe, err = NewZSTDBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}

	writeHandle.Close()
	return nil, nil, fmt.Errorf("unknown compression type '%s' (compression level %d)", *compressionType, *compressionLevel)
}

func startBackupPluginCommand() (*exec.Cmd, io.WriteCloser, error) {
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		return nil, nil, err
	}
	cmdStr := fmt.Sprintf("%s backup_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	writeCmd := exec.Command("bash", "-c", cmdStr)

	writeHandle, err := writeCmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	writeCmd.Stderr = &errBuf
	err = writeCmd.Start()
	if err != nil {
		return nil, nil, err
	}
	return writeCmd, writeHandle, nil
}
