package mybitcask

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.Nil(t, err)
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
		assert.Nil(t, err)
	}

	for i := 0; i <= 10000; i++ {
		v, err := db.Get(GetKey(rand.Intn(writeCnt)))
		assert.Nil(t, err)
		assert.NotNil(t, v)
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
		{
			name: "normal",
			db:   db,
			args: args{
				key: []byte("k-1"),
			},
			want:    []byte("v-1"),
			wantErr: false,
		},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.GetDel(tt.args.key)
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
