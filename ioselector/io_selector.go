package ioselector

import "os"

const FilePerm = 0644

type IOSelector interface {
	// Write a slice at given offset
	// Return the number of bytes written and error, if any
	Write(b []byte, offset int64) (int, error)
	// Read a slice from offset with fixed length
	// Return the number of bytes read and error, if any
	Read(b []byte, offset int64) (int, error)
	// Sync commits the current contents of the file to stable storage.
	// Typically, this means flushing the file system's in-memory copy
	// of recently written data to disk.
	Sync() error
	// Close the file
	// Return an error if file has already been closed
	Close() error
	// Delete the file
	// Must close the file before remove the file
	Delete() error
}

func openFile(filename string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// fill the original file with null bytes
	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
