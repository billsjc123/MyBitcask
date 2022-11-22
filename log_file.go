package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"billsjc/MyBitcask/ioselector"
	"os"
	"strconv"
	"sync"
)

var (
	DataTypeMap map[string]DataType = map[string]DataType{
		"strs": String,
		"list": List,
		"sets": Set,
		"hash": Hash,
		"zset": ZSet,
	}

	DataTypeStrMap map[DataType]string = map[DataType]string{
		String: "strs",
		List:   "list",
		Set:    "sets",
		Hash:   "hash",
		ZSet:   "zset",
	}
)

const (
	LogFilePrefix = "log."
)

type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IOSelector ioselector.IOSelector
}

func OpenLogFile(path string, fid uint32, filetype DataType, fsize int64) (*LogFile, error) {
	filename := getLogFileName(path, fid, filetype)
	ioselector, err := ioselector.NewFileIOSelector(filename, fsize)
	if err != nil {
		return nil, err
	}

	lf := &LogFile{
		Fid:        fid,
		IOSelector: ioselector,
	}
	return lf, nil
}

func getLogFileName(path string, fid uint32, filetype DataType) string {
	return path + string(os.PathSeparator) + LogFilePrefix + DataTypeStrMap[filetype] + "." + strconv.Itoa(int(fid))
}

// Read a log entry from given offset
// Return a LogEntry if offset is valid
// Return err:io.EOF if offset is invalid
func (f *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// read header from file
	headerbuf := make([]byte, MaxHeaderSize)
	_, err := f.IOSelector.Read(headerbuf, offset)
	if err != nil {
		return nil, 0, err
	}
	// decode header
	header, index := decodeHeader(headerbuf)

	// invalid header
	if index == 0 || header.crc32 == 0 || header.keySize == 0 || header.valSize == 0 {
		return nil, 0, consts.ErrEndOfEntry
	}

	entry := &LogEntry{
		key:      make([]byte, header.keySize),
		val:      make([]byte, header.valSize),
		expireAt: header.expireAt,
		typ:      header.typ,
	}

	kSize, err := f.IOSelector.Read(entry.key, offset+int64(index)+1)
	if err != nil {
		return nil, 0, err
	}

	vSize, err := f.IOSelector.Read(entry.val, offset+int64(index)+int64(kSize)+1)
	if err != nil {
		return nil, 0, err
	}

	return entry, int64(index) + int64(kSize) + int64(vSize), nil
}
