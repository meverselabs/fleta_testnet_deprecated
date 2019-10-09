package pof

import (
	"time"

	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/service/p2p"
)

func (fr *FormulatorNode) broadcastStatus() error {
	cp := fr.cs.cn.Provider()
	height, lastHash, _ := cp.LastStatus()
	nm := &p2p.StatusMessage{
		Version:  cp.Version(),
		Height:   height,
		LastHash: lastHash,
	}
	fr.ms.BroadcastMessage(nm)
	fr.broadcastMessage(0, nm)
	return nil
}

func (fr *FormulatorNode) sendRequestBlockTo(TargetID string, Height uint32) error {
	nm := &p2p.RequestMessage{
		Height: Height,
	}
	fr.ms.SendTo(TargetID, nm)
	fr.requestTimer.Add(Height, 2*time.Second, TargetID)
	return nil
}

func (fr *FormulatorNode) sendRequestBlockToNode(TargetPubHash common.PublicHash, Height uint32) error {
	if TargetPubHash == fr.myPublicHash {
		return nil
	}

	nm := &p2p.RequestMessage{
		Height: Height,
	}
	fr.sendMessage(0, TargetPubHash, nm)
	fr.requestNodeTimer.Add(Height, 10*time.Second, string(TargetPubHash[:]))
	return nil
}
