package p2p

import (
	"time"

	"github.com/fletaio/fleta_testnet/common"
)

func (nd *Node) sendStatusTo(TargetPubHash common.PublicHash) error {
	if TargetPubHash == nd.myPublicHash {
		return nil
	}

	cp := nd.cn.Provider()
	height, lastHash, _ := cp.LastStatus()
	nm := &StatusMessage{
		Version:  cp.Version(),
		Height:   height,
		LastHash: lastHash,
	}
	nd.sendMessage(0, TargetPubHash, nm)
	return nil
}

func (nd *Node) broadcastStatus() error {
	cp := nd.cn.Provider()
	height, lastHash, _ := cp.LastStatus()
	nm := &StatusMessage{
		Version:  cp.Version(),
		Height:   height,
		LastHash: lastHash,
	}
	nd.broadcastMessage(0, nm)
	return nil
}

func (nd *Node) sendRequestBlockTo(TargetPubHash common.PublicHash, Height uint32) error {
	if TargetPubHash == nd.myPublicHash {
		return nil
	}

	nm := &RequestMessage{
		Height: Height,
	}
	nd.sendMessage(0, TargetPubHash, nm)
	nd.requestTimer.Add(Height, 5*time.Second, string(TargetPubHash[:]))
	return nil
}
