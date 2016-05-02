package chredis

import(
  "crypto/rand"
  "encoding/hex"
  "fmt"
  "sync"
  "testing"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
)

func dial_and_create(t *testing.T) (*Rwriter, error) {
  w, err := Dial("tcp", "localhost:6379")
  require.Nil(t, err)
  return w, err
}

func randStr() string {
  b := make([]byte, 16)
  if _, err := rand.Read(b); err != nil {
    panic(err)
  }
  return hex.EncodeToString(b)
}

func TestCmd(t *testing.T) {
  w, err := dial_and_create(t)
  require.Nil(t, err)

  defer w.Close()

  // Test bogus command.
  r := w.Cmd("non-existant-command")
  assert.Equal(
    t,
    r.Err.Error(),
    "ERR unknown command 'non-existant-command'")
  assert.NotNil(t, r.Err)

  // Test HMSET.
  key := randStr()
  r = w.Cmd("HMSET", key, randStr(), randStr())
  assert.Nil(t, r.Err)
  r = w.Cmd("DEL", key)
  assert.Nil(t, r.Err)

  // Test write via channel.
  ch := make(chan Rhash)
  var wg sync.WaitGroup
  wg.Add(1)
  go w.ReceiveFromChannelAndWriteToServer(ch, &wg)
  randstr := randStr()
  for i := 0; i < 5; i++ {
    key = fmt.Sprintf("%s:%d", randstr, i)
    rhash := Rhash{key, map[string]string{"f1":"v1", "f2":"v2"}}
    ch <- rhash
  }
  close(ch)
  fmt.Println("Waiting for goroutine to finish...")
  wg.Wait()
  fmt.Println("Goroutine is done. End of test func.")
  // Clean-up.
  for i := 0; i < 5; i++ {
    key = fmt.Sprintf("%s:%d", randstr, i)
    r = w.Cmd("DEL", key)
    assert.Nil(t, r.Err)
  }
}
