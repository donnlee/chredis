package chredis

import (
	"sync"

	"github.com/mediocregopher/radix.v2/redis"
)

// Rwriter defines the methods a redis writer can do.
type Rwriter interface {
	Cmd(cmd string, args ...interface{}) *redis.Resp
	WriteRhash(h Rhash) *redis.Resp
	Close() error
	ReceiveFromChannelAndWriteToServer(ch chan Rhash, wg *sync.WaitGroup)
}

// Rclient is a wrapper for redis.Client
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

// Dial connects to the redis server (it's a wrapper).
func Dial(protocol, addr string) (*Rclient, error) {
	c, err := redis.Dial(protocol, addr)
	if err != nil {
		return nil, err
	}
	rc := Rclient{}
	rc.Client = *c
	return &rc, err
}

// Cmd is a wrapper of redis.Cmd().
func (rc *Rclient) Cmd(cmd string, args ...interface{}) *redis.Resp {
	return rc.Client.Cmd(cmd, args)
}

// WriteRhash writes a redis hash to the redis server.
func (rc *Rclient) WriteRhash(h Rhash) *redis.Resp {
	// redis.Cmd() accepts a slice of interface{}, so can't use []string.
	rhashAsSlice := []interface{}{h.Key}
	fvMapAsSlice := explodeMapStringString(h.FvMap)
	rhashAsSlice = append(rhashAsSlice, fvMapAsSlice...)
	// Send slice to variadic fn with triple-dots ("myslice...")
	return rc.Cmd("HMSET", rhashAsSlice...)
}

// Close the TCP connection.
func (rc *Rclient) Close() error {
	return rc.Client.Close()
}

// ReceiveFromChannelAndWriteToServer receives Rhash struct's from channel &
// writes them to redis server.
// Execute this as a goroutine and shove Rhash's at it. Close the channel
// when you are done and this func will complete/exit.
func (rc *Rclient) ReceiveFromChannelAndWriteToServer(
	ch chan Rhash, wg *sync.WaitGroup) {

	// Start the goroutine for resp checking.
	respCh := make(chan *redis.Resp)
	go rc.receiveRespAndCheckForNil(respCh, wg)

	for h := range ch {
		// Send resp to the next goroutine for checking success of the write.
		respCh <- rc.WriteRhash(h)
	}
	// We end up here when ch is closed.
	close(respCh)
}

/*
 * Receive response (resp) from ReceiveFromChannelAndWriteToServer and check
 * them for nil. When this func is done checking all write responses, it'll
 * lower the waitgroup count, unblocking the caller of
 * ReceiveFromChannelAndWriteToServer().
 * Panic can be recover()'d by callers of this func.
 */
func (rc *Rclient) receiveRespAndCheckForNil(
	respCh chan *redis.Resp, wg *sync.WaitGroup) {

	defer wg.Done()
	for resp := range respCh {
		if resp.Err != nil {
			// TODO: Do something better than panic.
			panic(resp.Err)
		}
	}
}
