package chredis_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestChredis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Chredis Suite")
}
