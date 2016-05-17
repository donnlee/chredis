package chredis

import(
  "fmt"
  "sync"
  "github.com/mediocregopher/radix.v2/redis"
)

type Rreader interface {
  Cmd(cmd string, args ...interface{}) *redis.Resp
  ReadRhash(key string) (Rhash, error)
  Close() error
  ReceiveFromChannelAndReadFromServer(
    key_ch chan string,
    wg *sync.WaitGroup,
    f func(rh_ch chan Rhash, wg *sync.WaitGroup))
  PrintRhashFromChannel(rh_ch chan Rhash, wg *sync.WaitGroup)
  PrettyPrintRhashFromChannel(rh_ch chan Rhash, wg *sync.WaitGroup)
}

// Read a redis hash from the redis server.
func (rc *Rclient) readHash(key string) (map[string]string, error) {
  resp := rc.Cmd("HGETALL", key)
  if redis.IsTimeout(resp) {
    fmt.Println("Got redis timeout error on key:", key)
    return nil, resp.Err
  } else if resp.IsType(redis.IOErr) {
    // TODO: If redis-server dies, we end up here. Add error handling.
    fmt.Println("Got redis IOErr on key:", key)
    return nil, resp.Err
  }
  return resp.Map()
}

// Given a key, read a hash, returning a Rhash struct.
func (rc *Rclient) ReadRhash(key string) (Rhash, error) {
  m, err := rc.readHash(key)
  if err != nil {
    // Just propagate err upstream for handling.
    return Rhash{}, err
  }
  return Rhash{key, m}, nil
}

/*
 * Given a channel, waitgroup, and goroutine "f" (func), this func retrieves
 * keys from the channel, fetches Rhash'es from the redis server, and
 * sends the Rhash'es to goroutine "f" for further processing (whatever
 * the goroutine "f" wants to do with them. For example, print them with
 * PrettyPrintRhashFromChannel().
 * Note, "f" should lower the waitgroup count, not this func.
 */
func (rc *Rclient) ReceiveFromChannelAndReadFromServer(
    key_ch chan string,
    wg *sync.WaitGroup,
    f func(rh_ch chan Rhash, wg *sync.WaitGroup)) {

  // Create a channel for the fetched Rhash'es.
  rh_chan := make(chan Rhash)
  // Start the goroutine that will receive Rash'es.
  go f(rh_chan, wg)

  for key := range key_ch {
    rh, err := rc.ReadRhash(key)
    // TODO: If redis server dies, we panic here.
    if err != nil {panic(err)}
    rh_chan <- rh
  }
  // We end up here when key_ch is closed.
  close(rh_chan)
}

/*
 * Simple goroutine for ReceiveFromChannelAndReadFromServer that simply
 * prints the retreived Rhash'es. Pass this func to 
 * ReceiveFromChannelAndReadFromServer and RFCARFS will send Rhash'es to
 * this func (PrintRhashFromChannel). Good for testing.
 */
func (rc *Rclient) PrintRhashFromChannel(
    rh_ch chan Rhash, wg *sync.WaitGroup) {

  defer wg.Done()

  for rh := range rh_ch {
    fmt.Println("PrintRhashFromChannel:", rh)
  }
  fmt.Println("Detected end of Rhash channel.")
}

/*
 * Simple goroutine for ReceiveFromChannelAndReadFromServer that
 * prints Rhash'es in human-readable form. Pass this func to
 * ReceiveFromChannelAndReadFromServer.
 * Demonstrates how RFCARFS can send to any goroutine easily.
 */
func (rc *Rclient) PrettyPrintRhashFromChannel(
    rh_ch chan Rhash, wg *sync.WaitGroup) {
  defer wg.Done()
  for rh := range rh_ch {
    k := rh.Key
    fv_map := rh.Fv_map

    if len(fv_map) > 0 {
      fmt.Println(k)
      for field, value := range fv_map {
        fmt.Printf("  %s -> %s\n", field, value)
      }
    } else {
      fmt.Printf("Key '%s' was not found in the redis db.\n", k)
    }
  }
  fmt.Println("Detected end of Rhash channel.")
}
