package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"billsjc/MyBitcask/logger"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
)

type MyBitcask struct {
	activeLogFile  map[DataType]*LogFile
	arcivedLogFile map[DataType]map[uint32]*LogFile
	fidMap         map[DataType][]uint32 // Sorted fid slice for each datatype. Only used when start up.
	options        Options
	strIndex       *StringIndex
	mu             *sync.RWMutex
	closed         uint32
}

type valuePos struct {
	fid       uint32
	offset    int64
	entrySize int64
}

func Open(option Options) *MyBitcask {
	// Check if directory path exist
	if _, err := os.Open(option.DBPath); err != nil {
		os.MkdirAll(option.DBPath, os.ModePerm)
	}
	// Read existed log file
	db := &MyBitcask{
		activeLogFile:  make(map[DataType]*LogFile),
		arcivedLogFile: make(map[int8]map[uint32]*LogFile),
		fidMap:         make(map[DataType][]uint32),
		options:        option,
		strIndex:       NewStringIndex(),
	}
	if err := db.loadLogFile(); err != nil {
		return nil
	}

	// update indexes
	if err := db.loadIndexFromLogFile(); err != nil {
		return nil
	}

	return db
}

func (db *MyBitcask) Close() error {
	// Sync and close log files

	// set closed

	return nil
}

func (db *MyBitcask) WriteLogEntry(entry *LogEntry, dataType DataType) error {
	// lock

	// get active file

	// encode header

	// write

	// update index
	return nil
}

func (db *MyBitcask) ReadLogEntry(index IndexNode, dataType DataType) *LogEntry {
	// read lock

	// read from files by index

	// decode

	return &LogEntry{}
}

func (db *MyBitcask) loadLogFile() error {
	// get all the file in DBPath
	files, err := ioutil.ReadDir(db.options.DBPath)
	if err != nil {
		return err
	}

	// update fidMap
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), LogFilePrefix) {
			continue
		}
		splitedFileName := strings.Split(file.Name(), ".")
		if len(splitedFileName) != consts.LogFileNameSplitedLength {
			return consts.ErrLogFileNameInvalid
		}

		fid, err := strconv.Atoi(splitedFileName[2])
		if err != nil {
			return consts.ErrLogFileNameInvalid
		}
		typ := DataType(DataTypeMap[splitedFileName[1]])
		db.fidMap[typ] = append(db.fidMap[typ], uint32(fid))
	}

	// update activeLogFile & acrivedLogFile
	for dtype, _ := range db.fidMap {
		sort.Slice(db.fidMap[dtype], func(i, j int) bool {
			return db.fidMap[dtype][i] < db.fidMap[dtype][j]
		})
		// simple strategy for choosing active log: the newest one
		fids := db.fidMap[dtype]
		for i, fid := range fids {
			lf, err := OpenLogFile(db.options.DBPath, fid, dtype, db.options.LogFileSizeThreshold)
			if err != nil {
				return err
			}
			if i == len(fids)-1 {
				db.activeLogFile[dtype] = lf
			} else {
				db.arcivedLogFile[dtype][fid] = lf
			}
		}
	}
	return nil
}

func (db *MyBitcask) loadIndexFromLogFile() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}

		for i, fid := range fids {
			var lf *LogFile
			if i == len(fids)-1 {
				lf = db.activeLogFile[dataType]
			} else {
				lf = db.arcivedLogFile[dataType][fid]
			}
			if lf == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, size, err := lf.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == consts.ErrEndOfEntry {
						break
					}
					logger.Fatalf("log file is nil, failed to open db")
				}

				valPos := &valuePos{
					fid:       fid,
					offset:    offset,
					entrySize: size,
				}
				db.buildIndex(dataType, entry, valPos)
				offset += size
			}

			if i == len(fids)-1 {
				lf.WriteAt = offset
			}
		}
	}

	wg := &sync.WaitGroup{}
	for dtype := String; dtype <= ZSet; dtype++ {
		wg.Add(1)
		iterateAndHandle(dtype, wg)
	}
	wg.Wait()
	return nil
}

func (db *MyBitcask) buildIndex(dataType DataType, entry *LogEntry, valPos *valuePos) {
	switch dataType {
	case String:
		db.buildStrIndex(entry, valPos)
	}
}

func (db *MyBitcask) buildStrIndex(entry *LogEntry, valPos *valuePos) {
	ts := time.Now().Unix()
	if entry.typ == TypeDelete || (entry.expireAt != 0 && ts > entry.expireAt) {
		db.strIndex.idxTree.Delete(art.Key(entry.key))
		return
	}

	indexNode := &IndexNode{
		fid:       valPos.fid,
		offset:    valPos.offset,
		entrySize: valPos.entrySize,
		expireAt:  entry.expireAt,
	}

	db.strIndex.idxTree.Insert(art.Key(entry.key), art.Value(indexNode))
}
