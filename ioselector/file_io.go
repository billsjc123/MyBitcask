package ioselector

import (
	"billsjc/MyBitcask/consts"
	"os"
)

type FileIOSelector struct {
	fd *os.File
}

func NewFileIOSelector(filename string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, consts.ErrLogFileNameInvalid
	}
	fd, err := openFile(filename, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: fd}, nil
}

func (fio *FileIOSelector) Write(b []byte, off int64) (int, error) {
	return fio.fd.WriteAt(b, off)
}

func (fio *FileIOSelector) Read(b []byte, off int64) (int, error) {
	return fio.fd.ReadAt(b, off)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

func (fio *FileIOSelector) Delete() error {
	if err := fio.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
