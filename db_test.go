package mybitcask

import (
	"billsjc/MyBitcask/logger"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/smartystreets/goconvey/convey"
)

var db *MyBitcask

var alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func TestMain(m *testing.M) {
	wd, err := os.Getwd()
	if err != nil {
		return
	}
	path := wd + string(os.PathSeparator) + "tmp"
	logger.Infof("DBPath: %s", path)
	option := &Options{
		DBPath:               path,
		LogFileSizeThreshold: 512 << 20,
	}

	db = Open(option)
	m.Run()
	db.Close()
}

func TestWriteLogEntry(t *testing.T) {
	convey.Convey("Write entry into log files", t, func() {
		var (
			in = &LogEntry{
				key: []byte("k1"),
				val: []byte("v1"),
			}
			expected = in
		)

		valPos, err := db.writeLogEntry(in, String)
		convey.So(err, convey.ShouldBeNil)

		err = updateIdxTree(db.strIndex.idxTree, valPos, in)
		convey.So(err, convey.ShouldBeNil)
		// update index
		value, ok := db.strIndex.idxTree.Search(art.Key("k1"))
		convey.So(ok, convey.ShouldBeTrue)

		index := value.(*IndexNode)
		convey.So(index.fid, convey.ShouldEqual, 1)

		// update log file
		out := db.readLogEntry(index, String)

		convey.So(out, convey.ShouldResemble, expected)
	})

	convey.Convey("Update key", t, func() {
		var (
			in1 = &LogEntry{
				key: []byte("k2"),
				val: []byte("v2_1"),
			}
			in2 = &LogEntry{
				key: []byte("k2"),
				val: []byte("v2_2"),
			}
			expected = []byte("v2_2")
		)

		valPos1, err := db.writeLogEntry(in1, String)
		convey.So(err, convey.ShouldBeNil)
		err = updateIdxTree(db.strIndex.idxTree, valPos1, in1)
		convey.So(err, convey.ShouldBeNil)

		valPos2, err := db.writeLogEntry(in2, String)
		convey.So(err, convey.ShouldBeNil)
		err = updateIdxTree(db.strIndex.idxTree, valPos2, in2)
		convey.So(err, convey.ShouldBeNil)

		// update index
		value, ok := db.strIndex.idxTree.Search(art.Key("k2"))
		convey.So(ok, convey.ShouldBeTrue)

		index := value.(*IndexNode)
		convey.So(index.fid, convey.ShouldEqual, 1)

		// update log file
		out := db.readLogEntry(index, String)

		convey.So(out.val, convey.ShouldResemble, expected)
	})
}

func TestReadLogEntry(t *testing.T) {
	convey.Convey("Read existed entry from log files", t, func() {
		var (
			expected = &LogEntry{
				key: []byte("k1"),
				val: []byte("v1"),
			}
		)

		// update index
		value, ok := db.strIndex.idxTree.Search(art.Key("k1"))
		convey.So(ok, convey.ShouldBeTrue)

		index := value.(*IndexNode)
		convey.So(index.fid, convey.ShouldEqual, 1)

		// update log file
		out := db.readLogEntry(index, String)

		convey.So(out, convey.ShouldResemble, expected)
	})
}

func destoryDB(db *MyBitcask) {
	if db == nil {
		return
	}
	db.Close()
	if runtime.GOOS == "windows" {
		time.Sleep(time.Millisecond * 100)
	}
	err := os.RemoveAll(db.options.DBPath)
	if err != nil {
		logger.Errorf("destroy db err: %v", err)
	}
}

func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}

func GetValue128B() []byte {
	return GetValue(128)
}
