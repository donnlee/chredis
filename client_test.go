package chredis

import(
  "crypto/rand"
  "encoding/hex"
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

  // Test bogus command.
  r := w.Cmd("non-existant-command")
  assert.Equal(
    t, r.Err.Error(), 
    "ERR unknown command 'non-existant-command'")
  assert.NotNil(t, r.Err)

  // Test HMSET.
  key := randStr()
  r = w.Cmd("HMSET", key, randStr(), randStr())
  assert.Nil(t, r.Err)
  r = w.Cmd("DEL", key)
  assert.Nil(t, r.Err)
}
