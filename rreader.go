package chredis

import(
  "fmt"
  "github.com/mediocregopher/radix.v2/redis"
)

type Rreader interface {
  Cmd(cmd string, args ...interface{}) *redis.Resp
  ReadRhash(key string) (Rhash, error)
  Close() error
}

// Read a redis hash from the redis server.
func (rc *Rclient) readHash(key string) (map[string]string, error) {
  m, err := rc.Cmd("HGETALL", key).Map()
  return m, err
}

// Given a key, read a hash, returning a Rhash struct.
func (rc *Rclient) ReadRhash(key string) (Rhash, error) {
  m, err := rc.readHash(key)
  if err != nil {
    // Just propagate err upstream for handling.
    return Rhash{}, err
  }
  fmt.Println("")
  return Rhash{key, m}, nil
}

// TODO: Goroutine that retrieves a stream of keys, reads hashes, and shoves Rhash'es into ch --> 2nd ch just prints the hashes (as a demo implementation).

