package main

import "github.com/fletaio/fleta_testnet/common"

var Addrs = []common.Address{}

func init() {
	for i := 0; i < 400; i++ {
		Addrs = append(Addrs, common.NewAddress(0, uint16(i+1000), 0))
	}
}
