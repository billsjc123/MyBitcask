package mybitcask

import (
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

type IndexNode struct {
	fid       uint32
	offset    int64
	entrySize int64
	expireAt  int64
}

type StringIndex struct {
	mu      *sync.RWMutex
	idxTree art.Tree
}

func NewStringIndex() *StringIndex {
	return &StringIndex{
		mu:      new(sync.RWMutex),
		idxTree: art.New(),
	}
}
