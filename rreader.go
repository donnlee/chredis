package chredis

import(
  "fmt"
  "github.com/mediocregopher/radix.v2/redis"
)

type Rreader interface {
  Cmd(cmd string, args ...interface{}) *redis.Resp
  ReadRhash(key string) (map[string]string, error)
  Close() error
}

// Read a redis hash from the redis server.
func (rc *Rclient) ReadRhash(key string) (map[string]string, error) {
  m, err := rc.Cmd("HGETALL", key).Map()
  fmt.Println(m)
  return m, err
}
