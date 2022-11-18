package consts

import (
	"errors"
)

var (
	ErrLogFileNameInvalid error = errors.New("Invalid Log File Name")
	ErrInvalidLogFileSize error = errors.New("Invalid LogFileSizeThreshold in options")

	// Log Entry
	ErrDecodeLogEntryHeader error = errors.New("Unable to decode EntryHeader from files")
	ErrEndOfEntry           error = errors.New("End of entry in Log File")
)
