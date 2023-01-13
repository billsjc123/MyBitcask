package mybitcask

import (
	"billsjc/MyBitcask/consts"
	"bytes"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestSet(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	type args struct {
		key []byte
		val []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
	}{
		{"nil-key", db, args{key: nil, val: []byte("v1")}, false},
		{"normal", db, args{key: []byte("k2"), val: []byte("k2")}, false},
		{"nil-value", db, args{key: []byte("k3"), val: nil}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Set(tt.args.key, tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSet_LogFileThreshold(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 32 << 20 // 32MB
	db := Open(opts)

	defer destoryDB(db)

	for i := 0; i <= 600000; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.NilError(t, err)
	}
}

func TestGet(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))
	db.Set([]byte("k-2"), []byte("v-2"))
	db.Set([]byte("k-3"), []byte("v-3"))
	db.Set([]byte("k-3"), []byte("v-333"))

	type args struct {
		key []byte
	}
	test := []struct {
		name    string
		db      *MyBitcask
		args    args
		want    []byte
		wantErr bool
	}{
		{"nil-key", db, args{key: nil}, nil, true},
		{"normal", db, args{key: []byte("k-1")}, []byte("v-1"), false},
		{"normal-rewrite", db, args{key: []byte("k-3")}, []byte("v-333"), false},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGet_LogFileThreshold(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 32 << 20 // 32MB
	db := Open(opts)

	defer destoryDB(db)

	writeCnt := 600000
	for i := 0; i <= writeCnt; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.NilError(t, err)
	}

	for i := 0; i <= 10000; i++ {
		v, err := db.Get(GetKey(rand.Intn(writeCnt)))
		assert.NilError(t, err)
		assert.Assert(t, v != nil)
	}
}

func TestMGet(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))
	db.Set([]byte("k-2"), []byte("v-2"))
	db.Set([]byte("k-3"), []byte("v-3"))
	db.Set([]byte("k-3"), []byte("v-333"))
	db.Set([]byte("k-4"), []byte("v-4"))
	db.Set([]byte("k-5"), []byte("v-5"))

	type args struct {
		keys [][]byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name:    "nil-key",
			db:      db,
			args:    args{keys: [][]byte{[]byte("nil")}},
			want:    [][]byte{nil},
			wantErr: false,
		},
		{
			name:    "normal",
			db:      db,
			args:    args{keys: [][]byte{[]byte("k-1")}},
			want:    [][]byte{[]byte("v-1")},
			wantErr: false,
		},
		{
			name:    "normal-rewrite",
			db:      db,
			args:    args{keys: [][]byte{[]byte("k-3")}},
			want:    [][]byte{[]byte("v-333")},
			wantErr: false,
		},
		{
			name: "multiple key",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("k-1"),
				[]byte("k-2"),
				[]byte("k-4"),
				[]byte("k-5"),
			}},
			want: [][]byte{
				[]byte("v-1"),
				[]byte("v-2"),
				[]byte("v-4"),
				[]byte("v-5"),
			},
			wantErr: false,
		},
		{
			name: "missed one key",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("k-1"),
				[]byte("missed-key1"),
				[]byte("k-2"),
			}},
			want: [][]byte{
				[]byte("v-1"),
				nil,
				[]byte("v-2"),
			},
			wantErr: false,
		},
		{
			name: "missed mutiple key",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("missed-key1"),
				[]byte("missed-key2"),
			}},
			want: [][]byte{
				nil,
				nil,
			},
			wantErr: false,
		},
		{
			name:    "empty key",
			db:      db,
			args:    args{keys: [][]byte{}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.MGet(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("MGet() error: %v, wantErr: %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("MGet() got = %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestGetRange(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k-1"), []byte("0123456789"))
	db.Set([]byte("k-empty"), []byte(""))

	type args struct {
		key        []byte
		start, end int
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: 3,
				end:   8,
			},
			want:    []byte("345678"),
			wantErr: false,
		},
		{
			name: "empty",
			db:   db,
			args: args{
				key:   []byte("k-empty"),
				start: 3,
				end:   8,
			},
			want:    []byte{},
			wantErr: false,
		},
		{
			name: "missed key",
			db:   db,
			args: args{
				key:   []byte("missed key"),
				start: 3,
				end:   8,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "start neg",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: -3,
				end:   9,
			},
			want:    []byte("789"),
			wantErr: false,
		},
		{
			name: "over start neg limits",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: -100,
				end:   3,
			},
			want:    []byte("0123"),
			wantErr: false,
		},
		{
			name: "end neg",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: 0,
				end:   -2,
			},
			want:    []byte("012345678"),
			wantErr: false,
		},
		{
			name: "over end limits",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: 7,
				end:   999,
			},
			want:    []byte("789"),
			wantErr: false,
		},
		{
			name: "start and end both positive, but end > start",
			db:   db,
			args: args{
				key:   []byte("k-1"),
				start: 9,
				end:   1,
			},
			want:    []byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.GetRange(tt.args.key, tt.args.start, tt.args.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRange() error: %v, wantErr: %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("GetRange() got = %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestGetDel(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k-1"), []byte("v-1"))

	type args struct {
		key []byte
	}
	test := []struct {
		name    string
		db      *MyBitcask
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				key: []byte("k-1"),
			},
			want:    []byte("v-1"),
			wantErr: false,
		},
		{
			name: "normal after delete",
			db:   db,
			args: args{
				key: []byte("k-1"),
			},
			want:    []byte{},
			wantErr: false,
		},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.GetDel(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetDel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDel() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))

	type args struct {
		keys []byte
	}
	test := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				keys: []byte("k-1"),
			},
			wantErr: false,
		},
		{
			name: "delete missing key",
			db:   db,
			args: args{
				keys: []byte("missing key"),
			},
			wantErr: false,
		},
		{
			name: "delete nil key",
			db:   db,
			args: args{
				keys: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Delete(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestSetEX(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	err := db.SetEX(GetKey(1), GetValue128B(), time.Second)
	assert.NilError(t, err)
	time.Sleep(time.Second * 2)
	v, err := db.Get(GetKey(1))
	assert.Equal(t, 0, len(v))
	assert.Equal(t, consts.ErrKeyNotFound, err)

	err = db.SetEX(GetKey(2), GetValue128B(), time.Second*3)
	assert.NilError(t, err)
	time.Sleep(time.Second)
	v1, err := db.Get(GetKey(2))
	assert.Assert(t, v1 != nil)
	assert.NilError(t, err)

	// Set an existed key
	err = db.Set(GetKey(3), GetValue128B())
	assert.NilError(t, err)

	err = db.SetEX(GetKey(3), GetValue128B(), time.Second)
	assert.NilError(t, err)
	time.Sleep(time.Second * 2)
	v2, err := db.Get(GetKey(3))
	assert.Equal(t, 0, len(v2))
	assert.Equal(t, consts.ErrKeyNotFound, err)
}

func TestSetNX(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("v1"))

	type args struct {
		key []byte
		val []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
	}{
		{"not exist key", db, args{key: []byte("not exist key"), val: []byte("value")}, false},
		{"exist key", db, args{key: []byte("k1"), val: []byte("new-v1")}, false},
		{"nil-value", db, args{key: nil, val: []byte("nil value")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Set(tt.args.key, tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("SetNX() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMSet(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	type args struct {
		args [][]byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
	}{
		{"normal", db, args{args: [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")}}, false},
		{"nil-key", db, args{args: [][]byte{nil, []byte("nil-key-v")}}, false},
		{"nil-value", db, args{args: [][]byte{[]byte("nil-value-key"), nil}}, false},
		{"odd number of args", db, args{args: [][]byte{[]byte("only have 1 args")}}, true},
		{"no args", db, args{args: [][]byte{}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.MSet(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("MSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMSetNX(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k10"), []byte("v10"))

	type args struct {
		args [][]byte
	}
	tests := []struct {
		name                   string
		db                     *MyBitcask
		args                   args
		wantErr                bool
		expectedDuplicateKey   []byte
		expectedDuplicateValue []byte
	}{
		{
			name:    "normal",
			db:      db,
			args:    args{args: [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")}},
			wantErr: false,
		},
		{
			name:    "nil-key",
			db:      db,
			args:    args{args: [][]byte{nil, []byte("nil-key-v")}},
			wantErr: false,
		},
		{
			name:    "nil-value",
			db:      db,
			args:    args{args: [][]byte{[]byte("nil-value-key"), nil}},
			wantErr: false,
		},
		{
			name:    "odd number of args",
			db:      db,
			args:    args{args: [][]byte{[]byte("only have 1 args")}},
			wantErr: true,
		},
		{
			name:    "no args",
			db:      db,
			args:    args{args: [][]byte{}},
			wantErr: true,
		},
		{
			name: "duplicate key",
			db:   db,
			args: args{args: [][]byte{
				[]byte("k4"), []byte("v4"),
				[]byte("k5"), []byte("v5"),
				[]byte("k4"), []byte("newv4"),
			}},
			wantErr:                false,
			expectedDuplicateKey:   []byte("k4"),
			expectedDuplicateValue: []byte("newv4"),
		},
		{
			name: "existed key",
			db:   db,
			args: args{args: [][]byte{
				[]byte("k10"), []byte("newk10"),
			}},
			wantErr:                false,
			expectedDuplicateKey:   []byte("k10"),
			expectedDuplicateValue: []byte("v10"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if err = tt.db.MSetNX(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("MSetNX() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != consts.ErrWrongNumOfArgs {
				t.Errorf("expected error = %v, got %v", consts.ErrWrongNumOfArgs, err)
			}
			if tt.expectedDuplicateKey != nil {
				v, _ := tt.db.Get(tt.expectedDuplicateKey)
				if !bytes.Equal(v, tt.expectedDuplicateValue) {
					t.Errorf("expected duplicate value = %v, got %v", tt.expectedDuplicateValue, v)
				}
			}
		})
	}
}

func TestAppend(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k10"), []byte("v10"))

	type args struct {
		key []byte
		val []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
		want    []byte
	}{
		{
			name: "not existed key",
			db:   db,
			args: args{
				key: []byte("k1"),
				val: []byte("v1"),
			},
			wantErr: false,
			want:    []byte("v1"),
		},
		{
			name: "existed key",
			db:   db,
			args: args{
				key: []byte("k10"),
				val: []byte(" got appended"),
			},
			wantErr: false,
			want:    []byte("v10 got appended"),
		},
		{
			name: "nil key",
			db:   db,
			args: args{
				key: nil,
				val: []byte("nil key value"),
			},
			wantErr: false,
		},
		{
			name: "nil value",
			db:   db,
			args: args{
				key: []byte("nil value key"),
				val: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Append(tt.args.key, tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.want != nil {
				got, _ := tt.db.Get(tt.args.key)
				if !bytes.Equal(got, tt.want) {
					t.Errorf("want = %v, got %v", tt.want, got)
				}
			}
		})
	}
}

func TestDecr(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("123"))
	db.Set([]byte("k-wrong-type"), []byte("string"))
	db.Set([]byte("k-min-int"), []byte(strconv.Itoa(math.MinInt64)))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
		expErr  error
		expVal  int64
		expByte []byte
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				key: []byte("k1"),
			},
			wantErr: false,
			expVal:  122,
			expByte: []byte("122"),
		},
		{
			name: "not existed key",
			db:   db,
			args: args{
				key: []byte("k-not-existed"),
			},
			wantErr: false,
			expVal:  -1,
			expByte: []byte("-1"),
		},
		{
			name: "not integer type",
			db:   db,
			args: args{
				key: []byte("k-wrong-type"),
			},
			wantErr: true,
			expErr:  consts.ErrWrongValueType,
			expByte: []byte("string"),
		},
		{
			name: "integer overflow",
			db:   db,
			args: args{
				key: []byte("k-min-int"),
			},
			wantErr: true,
			expErr:  consts.ErrIntegerOverflow,
			expByte: []byte(strconv.Itoa(math.MinInt64)),
		},
		{
			name: "nil key",
			db:   db,
			args: args{
				key: nil,
			},
			wantErr: true,
			expErr:  consts.ErrKeyIsNil,
			expVal:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Decr(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decr() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr == false && got != tt.expVal {
				t.Errorf("expected new value = %v, got = %v", tt.expVal, got)
			}
			val, _ := tt.db.Get(tt.args.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("expected byte = %s, got = %v", tt.expByte, string(val))
			}
		})
	}
}

func TestIncr(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("123"))
	db.Set([]byte("k-wrong-type"), []byte("string"))
	db.Set([]byte("k-max-int"), []byte(strconv.Itoa(math.MaxInt64)))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantErr bool
		expErr  error
		expVal  int64
		expByte []byte
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				key: []byte("k1"),
			},
			wantErr: false,
			expVal:  124,
			expByte: []byte("124"),
		},
		{
			name: "not existed key",
			db:   db,
			args: args{
				key: []byte("k-not-existed"),
			},
			wantErr: false,
			expVal:  1,
			expByte: []byte("1"),
		},
		{
			name: "not integer type",
			db:   db,
			args: args{
				key: []byte("k-wrong-type"),
			},
			wantErr: true,
			expErr:  consts.ErrWrongValueType,
			expByte: []byte("string"),
		},
		{
			name: "integer overflow",
			db:   db,
			args: args{
				key: []byte("k-max-int"),
			},
			wantErr: true,
			expErr:  consts.ErrIntegerOverflow,
			expByte: []byte(strconv.Itoa(math.MaxInt64)),
		},
		{
			name: "nil key",
			db:   db,
			args: args{
				key: nil,
			},
			wantErr: true,
			expErr:  consts.ErrKeyIsNil,
			expVal:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Incr(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Incr() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr == false && got != tt.expVal {
				t.Errorf("expected new value = %v, got = %v", tt.expVal, got)
			}
			val, _ := tt.db.Get(tt.args.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("expected byte = %s, got = %v", tt.expByte, string(val))
			}
		})
	}
}

func TestStrLen(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("123"))

	type args struct {
		Key []byte
	}
	tests := []struct {
		name string
		db   *MyBitcask
		args args
		want int
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				Key: []byte("k1"),
			},
			want: 3,
		},
		{
			name: "not exist key",
			db:   db,
			args: args{
				Key: []byte("not exist key"),
			},
			want: 0,
		},
		{
			name: "nil key",
			db:   db,
			args: args{Key: nil},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.db.StrLen(tt.args.Key)
			if got != tt.want {
				t.Errorf("want = %v, got %v", tt.want, got)
			}
		})
	}
}

func TestCount(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	expCnt := 0
	got := db.Count()
	assert.Equal(t, expCnt, got)

	db.Set([]byte("k1"), []byte("123"))
	db.Set([]byte("k2"), []byte("123"))
	db.Set([]byte("k3"), []byte("123"))

	expCnt = 3
	got = db.Count()
	assert.Equal(t, expCnt, got)

	db.Delete([]byte("k1"))
	expCnt = 2
	got = db.Count()
	assert.Equal(t, expCnt, got)
}

func TestScan(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("v1"))
	db.Set([]byte("k12"), []byte("v12"))
	db.Set([]byte("k123"), []byte("v123"))
	db.Set([]byte("k1234"), []byte("v1234"))
	db.Set([]byte("k2"), []byte("v2"))
	db.Set([]byte("different"), []byte("vd"))

	type args struct {
		prefix  []byte
		pattern string
		count   int
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name: "only prefix",
			db:   db,
			args: args{
				prefix:  []byte("k12"),
				pattern: "",
				count:   5,
			},
			want: [][]byte{
				[]byte("k12"), []byte("v12"),
				[]byte("k123"), []byte("v123"),
				[]byte("k1234"), []byte("v1234"),
			},
			wantErr: false,
		},
		{
			name: "only pattern",
			db:   db,
			args: args{
				prefix:  []byte(""),
				pattern: "k.*",
				count:   5,
			},
			want: [][]byte{
				[]byte("k1"), []byte("v1"),
				[]byte("k12"), []byte("v12"),
				[]byte("k123"), []byte("v123"),
				[]byte("k1234"), []byte("v1234"),
				[]byte("k2"), []byte("v2"),
			},
			wantErr: false,
		},
		{
			name: "nil prefix",
			db:   db,
			args: args{
				prefix:  nil,
				pattern: "",
				count:   5,
			},
			want:    [][]byte{},
			wantErr: false,
		},
		{
			name: "count < 0",
			db:   db,
			args: args{
				prefix:  []byte(""),
				pattern: "",
				count:   -1,
			},
			want:    [][]byte{},
			wantErr: false,
		},
		{
			name: "count > actual number of matched",
			db:   db,
			args: args{
				prefix:  []byte("k123"),
				pattern: "",
				count:   10,
			},
			want: [][]byte{
				[]byte("k123"), []byte("v123"),
				[]byte("k1234"), []byte("v1234"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Scan(tt.args.prefix, tt.args.pattern, tt.args.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("Scan got err: %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("want = %v, got %v", tt.want, got)
			}
		})
	}
}

func TestExpire(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("v1"))
	db.Set([]byte("k2"), []byte("v2"))

	type args struct {
		key      []byte
		duration time.Duration
	}
	tests := []struct {
		name      string
		db        *MyBitcask
		args      args
		sleepTime time.Duration
		want      []byte
		wantErr   bool
	}{
		{
			name: "normal",
			db:   db,
			args: args{
				key:      []byte("k1"),
				duration: time.Second,
			},
			want:      []byte(""),
			sleepTime: time.Second * 2,
			wantErr:   false,
		},
		{
			name: "non exist key",
			db:   db,
			args: args{
				key:      []byte("key not exist"),
				duration: time.Second,
			},
			want:      []byte(""),
			sleepTime: time.Second,
			wantErr:   false,
		},
		{
			name: "before expiration",
			db:   db,
			args: args{
				key:      []byte("k2"),
				duration: time.Second * 3,
			},
			want:      []byte("v2"),
			sleepTime: time.Second,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Expire(tt.args.key, tt.args.duration)
			if (err != nil) != tt.wantErr {
				t.Errorf("Expire got err: %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(tt.sleepTime)
			got, _ := tt.db.Get(tt.args.key)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("want %v, got %v", tt.want, got)
			}
		})
	}
}

func TestTTL(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	t1, err := db.TTL(GetKey(111))
	assert.Equal(t, int64(0), t1)
	assert.Equal(t, consts.ErrKeyNotFound, err)

	err = db.SetEX(GetKey(123), GetValue16B(), time.Second*30)
	assert.NilError(t, err)

	t2, err := db.TTL(GetKey(123))
	assert.Equal(t, int64(30), t2)
	assert.NilError(t, err)

	err = db.Set(GetKey(007), GetValue16B())
	assert.NilError(t, err)
	db.Expire(GetKey(007), time.Second*50)

	t3, err := db.TTL(GetKey(007))
	assert.Equal(t, int64(50), t3)
	assert.NilError(t, err)

	db.SetEX(GetKey(999), GetValue16B(), time.Second*5)
	db.Expire(GetKey(999), time.Second*100)
	db.Expire(GetKey(999), time.Second*10)

	t4, err := db.TTL(GetKey(999))
	assert.Equal(t, int64(10), t4)
	assert.NilError(t, err)
}

func TestPersist(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	db.Set([]byte("k1"), []byte("v1"))
	db.SetEX([]byte("k2"), []byte("v2"), time.Second*100)

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *MyBitcask
		args    args
		wantTTL int64
		wantErr bool
		expErr  error
	}{
		{
			name: "normal-not set expire",
			db:   db,
			args: args{
				key: []byte("k1"),
			},
			wantTTL: 0,
			wantErr: false,
		},
		{
			name: "normal-set expire",
			db:   db,
			args: args{
				key: []byte("k2"),
			},
			wantTTL: 0,
			wantErr: false,
		},
		{
			name: "not exist key",
			db:   db,
			args: args{
				key: []byte("key not exist"),
			},
			wantTTL: 0,
			wantErr: true,
			expErr:  consts.ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Persist(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Persist got err: %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != tt.expErr {
				t.Errorf("expect error: %v, got: %v", tt.expErr, err)
			}
			got, _ := tt.db.TTL(tt.args.key)
			if got != tt.wantTTL {
				t.Errorf("wantTTL %v, got %v", tt.wantTTL, got)
			}
		})
	}
}

func TestGetStrsKeys(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	opts := DefaultOptions(path)
	db := Open(opts)

	defer destoryDB(db)

	want := [][]byte{}
	got, err := db.GetStrsKeys()
	assert.NilError(t, err)
	assert.DeepEqual(t, got, want)

	db.Set([]byte("k1"), []byte("v1"))
	db.Set([]byte("k2"), []byte("v2"))
	want = [][]byte{[]byte("k1"), []byte("k2")}
	got, err = db.GetStrsKeys()
	assert.NilError(t, err)
	assert.DeepEqual(t, got, want)

	db.Delete([]byte("k1"))
	want = [][]byte{[]byte("k2")}
	got, err = db.GetStrsKeys()
	assert.NilError(t, err)
	assert.DeepEqual(t, got, want)
}
