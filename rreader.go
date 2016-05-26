package chredis

import (
	"fmt"
	"sync"

	"github.com/mediocregopher/radix.v2/redis"
)

// Rreader defines methods a redis reader can do.
type Rreader interface {
	Cmd(cmd string, args ...interface{}) *redis.Resp
	ReadRhash(key string) (Rhash, error)
	Close() error
	ReceiveFromChannelAndReadFromServer(
		keyCh chan string,
		errorCh chan error,
		wg *sync.WaitGroup,
		f func(rhCh chan Rhash, wg *sync.WaitGroup))
	PrintRhashFromChannel(rhCh chan Rhash, wg *sync.WaitGroup)
	PrettyPrintRhashFromChannel(rhCh chan Rhash, wg *sync.WaitGroup)
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

// ReadRhash returns a Rhash struct, given a key.
func (rc *Rclient) ReadRhash(key string) (Rhash, error) {
	m, err := rc.readHash(key)
	if err != nil {
		// Just propagate err upstream for handling.
		return Rhash{}, err
	}
	return Rhash{key, m}, nil
}

// ReceiveFromChannelAndReadFromServer retrieves keys from given channel,
// fetches Rhash'es from the redis server, and sends the Rhash'es to goroutine
// "f" for further processing (whatever "f" wants to do with them. For
// example, print them with example func PrettyPrintRhashFromChannel().
// Note, "f" should lower the waitgroup count, not this func.
func (rc *Rclient) ReceiveFromChannelAndReadFromServer(
	keyCh chan string,
	errorCh chan error,
	wg *sync.WaitGroup,
	f func(rhCh chan Rhash, wg *sync.WaitGroup)) {

	// Create a channel for the fetched Rhash'es.
	rhChan := make(chan Rhash)
	// Start the goroutine that will receive Rash'es.
	go f(rhChan, wg)

	for key := range keyCh {
		rh, err := rc.ReadRhash(key)
		// TODO: If redis server dies, we panic here. err is type *net.OpError
		// Plan: Propagate err to caller via channel. Then whoever does Dial() can reconnect.
		if err != nil {
			fmt.Printf("error type: %T\n", err)
			fmt.Println(err)
			//panic(err)
			errorCh <- err
			// If err, don't send empty/nil-laced rh to rhChan. Continue.
			continue
		}
		rhChan <- rh
	}
	// We end up here when keyCh is closed.
	close(rhChan)
}

// PrintRhashFromChannel is an example func for
// ReceiveFromChannelAndReadFromServer that simply prints the retrieved
// Rhash'es. Pass this func to RFCARFS and it will send Rhash'es to this
// func.
func (rc *Rclient) PrintRhashFromChannel(
	rhCh chan Rhash, wg *sync.WaitGroup) {

	defer wg.Done()

	for rh := range rhCh {
		fmt.Println("PrintRhashFromChannel:", rh)
	}
	fmt.Println("Detected end of Rhash channel.")
}

// PrettyPrintRhashFromChannel is a pretty version of PrintRhashFromChannel.
func (rc *Rclient) PrettyPrintRhashFromChannel(
	rhCh chan Rhash, wg *sync.WaitGroup) {
	defer wg.Done()
	for rh := range rhCh {
		k := rh.Key
		fvMap := rh.FvMap

		if len(fvMap) > 0 {
			fmt.Println(k)
			for field, value := range fvMap {
				fmt.Printf("  %s -> %s\n", field, value)
			}
		} else {
			fmt.Printf("Key '%s' was not found in the redis db.\n", k)
		}
	}
	fmt.Println("Detected end of Rhash channel.")
}
