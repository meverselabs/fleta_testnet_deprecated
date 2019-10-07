package pof

import (
	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/core/types"
)

type FormulatorAccount interface {
	types.Account
	IsFormulator() bool
	GeneratorHash() common.PublicHash
	IsActivated() bool
}
