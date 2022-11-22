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

// Write a byte slice at the end of a file
// Returns an error, if any
func (f *LogFile) Write(buf []byte) error {
	if buf == nil || len(buf) == 0 {
		return nil
	}
	n, err := f.IOSelector.Write(buf, f.WriteAt)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return consts.ErrWriteSizeNotEqual
	}
	return nil
}

// Read a file from offset with at a length of size
// Returns byte slice and error, if any
func (f *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size < 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := f.IOSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func getLogFileName(path string, fid uint32, filetype DataType) string {
	return path + string(os.PathSeparator) + LogFilePrefix + DataTypeStrMap[filetype] + "." + strconv.Itoa(int(fid))
}

// Read a log entry from given offset
// Return a LogEntry if offset is valid
// Return err:io.EOF if offset is invalid
func (f *LogFile) readLogEntry(offset int64) (*LogEntry, int64, error) {
	// read header from file
	headerbuf, err := f.Read(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	// decode header
	header, index := decodeHeader(headerbuf)

	// invalid header
	if index == 0 || header.crc32 == 0 || header.keySize == 0 || header.valSize == 0 {
		return nil, 0, consts.ErrEndOfEntry
	}

	key, err := f.Read(offset+int64(index)+1, header.keySize)
	if err != nil {
		return nil, 0, err
	}

	val, err := f.Read(offset+int64(index)+int64(header.keySize)+1, header.valSize)
	if err != nil {
		return nil, 0, err
	}

	entry := &LogEntry{
		key:      key,
		val:      val,
		expireAt: header.expireAt,
		typ:      header.typ,
	}

	return entry, int64(index) + int64(header.keySize) + int64(header.valSize), nil
}
