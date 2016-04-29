package chredis

import(
  "sync"
  "github.com/mediocregopher/radix.v2/redis"
)

type Rwriter struct {
  Client redis.Client
}

/*
 * Convert a map of field:value's to a slice of strings that'll later be
 * params when calling Cmd()
 */
func explodeMapStringString(m map[string]string) []interface{} {
  arr := []interface{}{}
  for k, v := range m {
    arr = append(arr, k)
    arr = append(arr, v)
  }
  return arr
}

/*
 * Connect to redis server and return a new redis writer.
 */
func Dial(protocol, addr string) (*Rwriter, error) {
  c, err := redis.Dial(protocol, addr)
  w := Rwriter{}
  w.Client = *c
  return &w, err
}

// Execute a redis command.
func (w *Rwriter) Cmd(cmd string, args ...interface{}) *redis.Resp {
  return w.Client.Cmd(cmd, args)
}

// Write a redis hash to the redis server.
func (w *Rwriter) WriteRhash(h Rhash) *redis.Resp {
  // redis.Cmd() accepts a slice of interface{}, so can't use []string.
  rhash_as_slice := []interface{}{h.Key}
  fv_map_as_slice := explodeMapStringString(h.Fv_map)
  rhash_as_slice = append(rhash_as_slice, fv_map_as_slice...)
  // Send slice to variadic fn with triple-dots ("myslice...")
  return w.Cmd("HMSET", rhash_as_slice...)
}

// Close the TCP connection.
func (w *Rwriter) Close() error {
  return w.Client.Close()
}

/*
 * Receive Rhash struct's from channel & write them to redis server.
 * Execute this as a goroutine and shove Rhash's at it. Close the channel
 * when you are done and this func will complete/exit.
 */
func (w *Rwriter) ReceiveFromChannelAndWriteToServer(
    ch chan Rhash, wg *sync.WaitGroup) {

  defer wg.Done()
  for h := range ch {
    w.WriteRhash(h)
  }
}
