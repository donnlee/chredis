package chredis

// Rhash represents a redis hash as a golang struct.
type Rhash struct {
	Key   string
	FvMap map[string]string
}
