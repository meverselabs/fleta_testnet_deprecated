package payment

import (
	"github.com/fletaio/fleta_testnet/common/hash"
	"github.com/fletaio/fleta_testnet/common/util"
)

// Topic returns the topic of the name
func Topic(Name string) uint64 {
	h := hash.Hash([]byte("fleta.payment#Topic#" + Name))
	return util.BytesToUint64(h[:])
}
