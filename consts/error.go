package consts

import (
	"errors"
)

var (
	ErrLogFileNameInvalid error = errors.New("Invalid Log File Name")

	ErrInvalidLogFileSize error = errors.New("Invalid LogFileSizeThreshold in options")

	ErrWriteSizeNotEqual error = errors.New("Write size is not equal to entry size")

	ErrDecodeLogEntryHeader error = errors.New("Unable to decode EntryHeader from files")

	ErrEndOfEntry error = errors.New("End of entry in Log File")

	ErrInvalidIndexTreeNode error = errors.New("Index Tree Node can not be converted to index")

	ErrKeyNotFound error = errors.New("Key is not found")

	ErrKeyIsNil error = errors.New("Key is nil")

	ErrWrongNumOfArgs error = errors.New("Number of argument is not correct")

	ErrWrongValueType error = errors.New("Value is not a Integer")

	ErrIntegerOverflow error = errors.New("Increment of decrment overflow")
)
