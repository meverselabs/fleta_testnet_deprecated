package types

import (
	"encoding/json"

	"github.com/fletaio/fleta_testnet/common"
)

// Account defines common account functions
type Account interface {
	json.Marshaler
	Address() common.Address
	Name() string
	Clone() Account
	Validate(loader LoaderWrapper, signers []common.PublicHash) error
}
