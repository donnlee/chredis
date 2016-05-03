package chredis

import(
  "sync"
  "github.com/mediocregopher/radix.v2/redis"
)

// Methods a redis writer can do.
type Rwriter interface {
  Cmd(cmd string, args ...interface{}) *redis.Resp
  WriteRhash(h Rhash) *redis.Resp
  Close() error
  ReceiveFromChannelAndWriteToServer(ch chan Rhash, wg *sync.WaitGroup)
  ReceiveRespAndCheckForNil(resp_ch chan *redis.Resp, wg *sync.WaitGroup)
}

type Rclient struct {
  Client redis.Client
}

/*
 * Helper func:
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
 * Connect to redis server and return a new redis client.
 */
func Dial(protocol, addr string) (*Rclient, error) {
  c, err := redis.Dial(protocol, addr)
  rc := Rclient{}
  rc.Client = *c
  return &rc, err
}

// Execute a redis command.
func (rc *Rclient) Cmd(cmd string, args ...interface{}) *redis.Resp {
  return rc.Client.Cmd(cmd, args)
}

// Write a redis hash to the redis server.
func (rc *Rclient) WriteRhash(h Rhash) *redis.Resp {
  // redis.Cmd() accepts a slice of interface{}, so can't use []string.
  rhash_as_slice := []interface{}{h.Key}
  fv_map_as_slice := explodeMapStringString(h.Fv_map)
  rhash_as_slice = append(rhash_as_slice, fv_map_as_slice...)
  // Send slice to variadic fn with triple-dots ("myslice...")
  return rc.Cmd("HMSET", rhash_as_slice...)
}

// Close the TCP connection.
func (rc *Rclient) Close() error {
  return rc.Client.Close()
}

/*
 * Receive Rhash struct's from channel & write them to redis server.
 * Execute this as a goroutine and shove Rhash's at it. Close the channel
 * when you are done and this func will complete/exit.
 */
func (rc *Rclient) ReceiveFromChannelAndWriteToServer(
    ch chan Rhash, wg *sync.WaitGroup) {

  // Start the goroutine for resp checking.
  resp_ch := make(chan *redis.Resp)
  go rc.ReceiveRespAndCheckForNil(resp_ch, wg)

  for h := range ch {
    // Send resp to the next goroutine for checking success of the write.
    resp_ch <- rc.WriteRhash(h)
  }
  // We end up here when ch is closed.
  close(resp_ch)
}

/*
 * Receive response (resp) from ReceiveFromChannelAndWriteToServer and check
 * them for nil. When this func is done checking all write responses, it'll
 * lower the waitgroup count, unblocking the caller of
 * ReceiveFromChannelAndWriteToServer().
 * Panic can be recover()'d by callers of this func.
 */
func (rc *Rclient) ReceiveRespAndCheckForNil(
    resp_ch chan *redis.Resp, wg *sync.WaitGroup) {

  defer wg.Done()
  for resp := range resp_ch {
    if resp.Err != nil {
      panic(resp.Err)
    }
  }
}
