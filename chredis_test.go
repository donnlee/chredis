package chredis_test

import (
	"encoding/hex"
	"math/rand"

	"github.com/donnlee/chredis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

var _ = Describe("Chredis", func() {
	var (
		w   chredis.Rwriter
		err error
		key string
	)

	BeforeEach(func() {
		w, err = chredis.Dial("tcp", "localhost:6379")
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Test redis Cmd", func() {
		Context("A non-existant command", func() {
			It("should throw an error", func() {
				resp := w.Cmd("non-existant-command")
				Expect(resp.Err).To(HaveOccurred())
				Expect(resp.Err.Error()).Should(ContainSubstring("ERR unknown command"))
			})

		})

		Context("A legit command, HMSET", func() {
			It("should set a hash", func() {
				key = randStr()
				resp := w.Cmd("HMSET", key, randStr(), randStr())
				Expect(resp.Err).ToNot(HaveOccurred())
			})
		})
		Context("Delete test hash", func() {
			It("should delete a hash", func() {
				resp := w.Cmd("DEL", key)
				Expect(resp.Err).ToNot(HaveOccurred())
			})
		})
	})

})
