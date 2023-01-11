package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"billsjc/MyBitcask/logger"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
)

type MyBitcask struct {
	activeLogFile  map[DataType]*LogFile
	arcivedLogFile map[DataType]map[uint32]*LogFile
	fidMap         map[DataType][]uint32 // Sorted fid slice for each datatype. Only used when start up.
	options        *Options
	strIndex       *StringIndex
	mu             *sync.RWMutex
	closed         uint32
}

type valuePos struct {
	fid       uint32
	offset    int64
	entrySize int64
}

func Open(option *Options) *MyBitcask {
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
		mu:             new(sync.RWMutex),
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
	db.mu.Lock()
	defer db.mu.Unlock()
	// Sync and close log files
	for _, activeFile := range db.activeLogFile {
		err := activeFile.IOSelector.Sync()
		if err != nil {
			logger.Fatalf("Sync log file error: %s", err)
			return err
		}
		activeFile.IOSelector.Close()
	}

	for _, archivedFiles := range db.arcivedLogFile {
		for _, file := range archivedFiles {
			err := file.IOSelector.Sync()
			if err != nil {
				logger.Fatalf("Sync log file error: %s", err)
				return err
			}
			file.IOSelector.Close()
		}
	}
	// set closed
	atomic.AddUint32(&db.closed, 1)
	return nil
}

func (db *MyBitcask) writeLogEntry(entry *LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}

	activeLogFile := db.getActiveLogFile(dataType)
	// encode header
	buf, size := entry.Encode()

	// if file size larger than threshold, create a new file
	if activeLogFile.WriteAt+int64(size) > db.options.LogFileSizeThreshold {
		if err := activeLogFile.IOSelector.Sync(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		newFid := db.activeLogFile[dataType].Fid + 1
		newActiveLogFile, err := OpenLogFile(db.options.DBPath, newFid, dataType, db.options.LogFileSizeThreshold)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		if db.arcivedLogFile[dataType] == nil {
			db.arcivedLogFile[dataType] = make(map[uint32]*LogFile)
		}
		db.arcivedLogFile[dataType][activeLogFile.Fid] = activeLogFile
		db.activeLogFile[dataType] = newActiveLogFile
		db.mu.Unlock()
	}

	// write
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	n, err := activeLogFile.IOSelector.Write(buf, writeAt)
	if err != nil {
		logger.Errorf("Write Log Entry Failed: %v", err)
		return nil, err
	}
	// update index
	valPos := &valuePos{
		fid:       activeLogFile.Fid,
		offset:    activeLogFile.WriteAt,
		entrySize: int64(size),
	}

	// update file writeAt
	activeLogFile.WriteAt += int64(n)

	return valPos, nil
}

func (db *MyBitcask) readLogEntry(index *IndexNode, dataType DataType) *LogEntry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// read from files by index
	var lf *LogFile
	if index.fid == db.activeLogFile[dataType].Fid {
		lf = db.activeLogFile[dataType]
	} else {
		var ok bool
		lf, ok = db.arcivedLogFile[dataType][index.fid]
		if !ok {
			logger.Errorf("Can't find Log File with fid: %v, Datatype: %v", index.fid, dataType)
			return nil
		}
	}

	entry, _, err := lf.readLogEntry(index.offset)
	if err != nil {
		return &LogEntry{}
	}

	return entry
}

func (db *MyBitcask) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeLogFile[dataType] != nil {
		return nil
	}

	var err error
	lf, err := OpenLogFile(db.options.DBPath, 1, dataType, db.options.LogFileSizeThreshold)
	if err != nil {
		return err
	}
	db.activeLogFile[dataType] = lf
	return nil
}

func (db *MyBitcask) loadLogFile() error {
	// get all the file in DBPath
	files, err := os.ReadDir(db.options.DBPath)
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
				if db.arcivedLogFile[dtype] == nil {
					db.arcivedLogFile[dtype] = make(map[uint32]*LogFile)
				}
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
				entry, size, err := lf.readLogEntry(offset)
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

func (db *MyBitcask) getActiveLogFile(dataType DataType) *LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFile[dataType]
}

func updateIdxTree(idxTree art.Tree, valPos *valuePos, entry *LogEntry) error {
	indexNode := &IndexNode{
		fid:       valPos.fid,
		offset:    valPos.offset,
		entrySize: valPos.entrySize,
	}
	if entry.expireAt != 0 {
		indexNode.expireAt = entry.expireAt
	}
	idxTree.Insert(art.Key(entry.key), art.Value(indexNode))
	return nil
}

func (db *MyBitcask) buildIndex(dataType DataType, entry *LogEntry, valPos *valuePos) {
	if entry.expireAt != 0 && entry.expireAt < time.Now().Unix() {
		return
	}
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

func (db *MyBitcask) getVal(idxTree art.Tree, key []byte, dataType DataType) ([]byte, error) {
	v, ok := idxTree.Search(art.Key(key))
	if !ok {
		return nil, consts.ErrKeyNotFound
	}

	index, _ := v.(*IndexNode)
	ts := time.Now().UnixMilli()
	if index.expireAt != 0 && ts > index.expireAt {
		return nil, consts.ErrKeyNotFound
	}

	entry := db.readLogEntry(index, String)
	return entry.val, nil
}

func (db *MyBitcask) getIndexNode(idxTree art.Tree, key []byte) (*IndexNode, error) {
	v, ok := idxTree.Search(art.Key(key))
	if !ok {
		return nil, consts.ErrKeyNotFound
	}

	index, _ := v.(*IndexNode)
	ts := time.Now().Unix()
	// lazy delete
	if index.expireAt != 0 && ts > index.expireAt {
		idxTree.Delete(art.Key(key))
		return nil, consts.ErrKeyNotFound
	}

	return index, nil
}
