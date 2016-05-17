package chredis

// Represent a redis hash as a Go/golang struct.
type Rhash struct {
	Key    string
	Fv_map map[string]string
}
