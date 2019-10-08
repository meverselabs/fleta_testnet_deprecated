package app

import (
	"strconv"

	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/amount"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/process/vault"
)

func setupSingleAccunt(sp *vault.Vault, ctw *types.ContextWrapper) {
	for i := 0; i < 20000; i++ {
		addSingleAccount(sp, ctw, common.MustParsePublicHash("2RqGkxiHZ4NopN9QxKgw93RuSrxX2NnLjv1q1aFDdV9"), common.NewAddress(0, uint16(i+40000), 0), strconv.Itoa(i+1000), amount.MustParseAmount("10000000"))
	}
}
