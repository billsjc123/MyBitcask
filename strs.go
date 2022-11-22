package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"bytes"
	"math"
	"regexp"
	"strconv"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
)

// Set set the key to hold the string value. Existing key will be overwritten
// Any TTL will be discarded on successful Set operation
func (db *MyBitcask) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	entry := &LogEntry{
		key: key,
		val: value,
	}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
	if err != nil {
		return err
	}
	return nil
}

// Get get the latest string value by key
// If the key cannot be found or is expired, return ErrKeyNotFound
func (db *MyBitcask) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

// MGet batch load string value by keys
// Return values in the order of keys
func (db *MyBitcask) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, consts.ErrWrongNumOfArgs
	}
	result := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && err != consts.ErrKeyNotFound {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

// GetRange returns the substring of string value stored at key
// determined by offsets start and end, including start and end pos
func (db *MyBitcask) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return []byte{}, nil
	}

	// Negative offsets can be used in order to provide an offset starting from the end of the string.
	// So -1 means the last character, -2 the penultimate and so forth
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}

	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > end {
		return []byte{}, nil
	}

	return val[start : end+1], nil
}

// GetDel returns stored value of specific key and delete the key
func (db *MyBitcask) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && err != consts.ErrKeyNotFound {
		return nil, err
	}
	if err == consts.ErrKeyNotFound {
		return []byte{}, nil
	}

	// delete
	delEntry := &LogEntry{
		key: key,
		typ: TypeDelete,
	}
	_, err = db.writeLogEntry(delEntry, String)
	if err != nil {
		return nil, err
	}

	db.strIndex.idxTree.Delete(art.Key(delEntry.key))
	return val, nil
}

// Delete delete value at the given key
func (db *MyBitcask) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// delete
	delEntry := &LogEntry{
		key: key,
		typ: TypeDelete,
	}
	_, err := db.writeLogEntry(delEntry, String)
	if err != nil {
		return err
	}

	db.strIndex.idxTree.Delete(art.Key(delEntry.key))
	return nil
}

// SetEX set key to hold the string value and
// set key to timeout after given duration
func (db *MyBitcask) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &LogEntry{
		key:      key,
		val:      value,
		expireAt: time.Now().Add(duration).Unix(),
	}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
	if err != nil {
		return err
	}
	return nil
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *MyBitcask) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	_, ok := db.strIndex.idxTree.Search(art.Key(key))
	if ok {
		return nil
	}

	entry := &LogEntry{
		key: key,
		val: value,
	}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
	if err != nil {
		return err
	}
	return nil
}

// MSet is multiple set command. Parameter order should be like "key", "value", "key", "value", ...
func (db *MyBitcask) MSet(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return consts.ErrWrongNumOfArgs
	}

	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		entry := &LogEntry{
			key: key,
			val: value,
		}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}

		err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
		if err != nil {
			return err
		}
	}

	return nil
}

// MSetNX sets given keys to their respective values. MSetNX will not perform
// any operation at all even if just a single key already exists.
func (db *MyBitcask) MSetNX(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return consts.ErrWrongNumOfArgs
	}

	// If any key already exists, discard other key-pair values
	// ensures atomicity
	for i := 0; i < len(args); i += 2 {
		key := args[i]
		if _, ok := db.strIndex.idxTree.Search(art.Key(key)); ok {
			return nil
		}
	}

	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		entry := &LogEntry{
			key: key,
			val: value,
		}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
		if err != nil {
			return err
		}
	}

	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *MyBitcask) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return err
	}
	if oldVal != nil {
		value = append(oldVal, value...)
	}
	entry := &LogEntry{
		key: key,
		val: value,
	}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
	if err != nil {
		return err
	}

	return nil
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *MyBitcask) Decr(key []byte) (int64, error) {
	return db.incrDecrBy(key, -1)
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *MyBitcask) DecrBy(key []byte, decr int64) (int64, error) {
	return db.incrDecrBy(key, -decr)
}

// Incr increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *MyBitcask) Incr(key []byte) (int64, error) {
	return db.incrDecrBy(key, 1)
}

// IncrBy increments the number stored at key by incr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *MyBitcask) IncrBy(key []byte, incr int64) (int64, error) {
	return db.incrDecrBy(key, incr)
}

// helper method for Decr and Incr
func (db *MyBitcask) incrDecrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && err != consts.ErrKeyNotFound {
		return 0, err
	}

	if err == consts.ErrKeyNotFound || bytes.Equal(oldVal, nil) {
		oldVal = []byte("0")
	}

	valInt64, err := strconv.ParseInt(string(oldVal), 10, 64)
	if err != nil {
		return 0, consts.ErrWrongValueType
	}

	if (valInt64 < 0 && incr < 0 && incr < (math.MinInt64-valInt64)) ||
		(valInt64 > 0 && incr > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, consts.ErrIntegerOverflow
	}

	valInt64 += incr
	newVal := []byte(strconv.FormatInt(valInt64, 10))
	entry := &LogEntry{
		key: key,
		val: newVal,
	}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = updateIdxTree(db.strIndex.idxTree, valPos, entry)
	if err != nil {
		return 0, err
	}

	return valInt64, nil
}

// StrLen returns the length of the string value stored at key. If the key
// doesn't exist, it returns 0.
func (db *MyBitcask) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}

	return len(val)
}

// Count returns the total number of keys of String.
func (db *MyBitcask) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will match key`s prefix, and pattern is a regular expression that also matchs the key.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key1, value1, key2, value2, etc...].
func (db *MyBitcask) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 || db.strIndex.idxTree == nil {
		return [][]byte{}, nil
	}

	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return [][]byte{}, err
		}
	}
	results := make([][]byte, 0)

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	db.strIndex.idxTree.ForEachPrefix(art.Key(prefix), func(node art.Node) (cont bool) {
		if node.Kind() != art.Leaf {
			return true
		}
		if !reg.Match(node.Key()) {
			return true
		}
		val, err := db.getVal(db.strIndex.idxTree, node.Key(), String)
		if err != nil {
			return true
		}
		results = append(results, node.Key(), val)
		if len(results) == count*2 {
			return false
		}
		return true
	})

	return results, nil
}

// Expire set the expiration time for the given key.
func (db *MyBitcask) Expire(key []byte, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	db.strIndex.mu.RLock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.RUnlock()
		return err
	}
	db.strIndex.mu.RUnlock()
	return db.SetEX(key, val, duration)
}

// TTL get ttl(time to live) for the given key.
func (db *MyBitcask) TTL(key []byte) (int64, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	index, err := db.getIndexNode(db.strIndex.idxTree, key)
	if err != nil {
		return 0, err
	}
	var ttl int64
	if index.expireAt != 0 {
		ttl = index.expireAt - time.Now().Unix()
	}
	return ttl, nil
}

// Persist remove the expiration time for the given key.
func (db *MyBitcask) Persist(key []byte) error {
	db.strIndex.mu.RLock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.RUnlock()
		return err
	}
	db.strIndex.mu.RUnlock()
	return db.Set(key, val)
}

// GetStrsKeys get all stored keys of type String.
func (db *MyBitcask) GetStrsKeys() ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return [][]byte{}, nil
	}
	keys := make([][]byte, 0)
	for it, i := db.strIndex.idxTree.Iterator(), 0; it.HasNext(); i++ {
		value, _ := it.Next()
		index, err := db.getIndexNode(db.strIndex.idxTree, value.Key())
		if err != nil && err != consts.ErrKeyNotFound {
			return nil, err
		}
		if index == nil {
			continue
		}
		keys = append(keys, value.Key())
	}
	return keys, nil
}
