package types

import (
	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/hash"
)

// BlockSign is the generator signature of the block
type BlockSign struct {
	HeaderHash         hash.Hash256
	GeneratorSignature common.Signature
}
