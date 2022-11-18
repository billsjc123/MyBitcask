package mybitcask

type Options struct {
	// MyBitcask directory path for all log file. Must be set
	// when intializing an MyBitcask instance.
	DBPath string

	// The maximum size of an active LogFile. If it reaches this
	// size, a new active LogFile should be created. Default value
	// is 512MB
	LogFileSizeThreshold int64
}

func DefaultOptions(path string) *Options {
	return &Options{
		DBPath:               path,
		LogFileSizeThreshold: 512 << 20,
	}
}
