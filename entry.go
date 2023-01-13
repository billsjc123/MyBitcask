package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"encoding/binary"
	"hash/crc32"
)

type EntryType byte

const (
	TypeDelete EntryType = iota + 1 // In this scenario only delete is needed
)

const (
	MaxHeaderSize = 25
)

type entryHeader struct {
	crc32    uint32 // checksum
	typ      EntryType
	keySize  uint32 // len of key
	valSize  uint32 // len of value
	expireAt int64  // expiration timestamp
}

type LogEntry struct {
	key      []byte
	val      []byte
	expireAt int64 // Time Unix
	typ      EntryType
}

func (e *LogEntry) Encode() ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
	header[4] = byte(e.typ)
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(e.key)))
	index += binary.PutVarint(header[index:], int64(len(e.val)))
	index += binary.PutVarint(header[index:], e.expireAt)

	size := index + len(e.key) + len(e.val)
	buf := make([]byte, size)
	copy(buf[:index], header)
	copy(buf[index:], e.key)
	copy(buf[index+len(e.key):], e.val)

	// checksum
	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[4:]))
	return buf, size
}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &entryHeader{}
	header.crc32 = binary.BigEndian.Uint32(buf[:4])
	header.typ = EntryType(buf[4])

	var index = 5
	kSize, n := binary.Varint(buf[index:])
	index += n
	vSize, n := binary.Varint(buf[index:])
	index += n
	expireAt, n := binary.Varint(buf[index:])
	index += n

	header.keySize = uint32(kSize)
	header.valSize = uint32(vSize)
	header.expireAt = int64(expireAt)
	return header, int64(index)
}

func DecodeEntry(buf []byte) (*LogEntry, error) {
	// decode header
	header, index := decodeHeader(buf)

	// invalid header
	if index == 0 || header.crc32 == 0 || header.keySize == 0 || header.valSize == 0 {
		return nil, consts.ErrDecodeLogEntryHeader
	}

	entry := &LogEntry{
		key:      make([]byte, header.keySize),
		val:      make([]byte, header.valSize),
		expireAt: header.expireAt,
		typ:      header.typ,
	}

	copy(entry.key, buf[index+int64(header.keySize):])
	copy(entry.val, buf[index+int64(header.keySize)+int64(header.valSize):])
	return entry, nil
}
