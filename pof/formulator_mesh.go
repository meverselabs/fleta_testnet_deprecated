package pof

import (
	crand "crypto/rand"
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/debug"
	"github.com/fletaio/fleta_testnet/common/hash"
	"github.com/fletaio/fleta_testnet/common/key"
	"github.com/fletaio/fleta_testnet/common/rlog"
	"github.com/fletaio/fleta_testnet/core/chain"
	"github.com/fletaio/fleta_testnet/service/p2p"
	"github.com/fletaio/fleta_testnet/service/p2p/peer"
)

type FormulatorNodeMesh struct {
	sync.Mutex
	fr            *FormulatorNode
	key           key.Key
	netAddressMap map[common.PublicHash]string
	peerMap       map[string]peer.Peer
}

func NewFormulatorNodeMesh(key key.Key, NetAddressMap map[common.PublicHash]string, fr *FormulatorNode) *FormulatorNodeMesh {
	ms := &FormulatorNodeMesh{
		key:           key,
		netAddressMap: NetAddressMap,
		peerMap:       map[string]peer.Peer{},
		fr:            fr,
	}
	return ms
}

// Run starts the formulator mesh
func (ms *FormulatorNodeMesh) Run() {
	for PubHash, v := range ms.netAddressMap {
		go func(pubhash common.PublicHash, NetAddr string) {
			time.Sleep(1 * time.Second)
			for {
				ms.Lock()
				_, has := ms.peerMap[string(pubhash[:])]
				ms.Unlock()
				if !has {
					if err := ms.client(NetAddr, pubhash); err != nil {
						rlog.Println("[client]", err, NetAddr)
					}
				}
				time.Sleep(1 * time.Second)
			}
		}(PubHash, v)
	}
}

// Peers returns peers of the formulator mesh
func (ms *FormulatorNodeMesh) Peers() []peer.Peer {
	ms.Lock()
	defer ms.Unlock()

	peers := []peer.Peer{}
	for _, p := range ms.peerMap {
		peers = append(peers, p)
	}
	return peers
}

// RemovePeer removes peers from the mesh
func (ms *FormulatorNodeMesh) RemovePeer(ID string) {
	ms.Lock()
	p, has := ms.peerMap[ID]
	if has {
		delete(ms.peerMap, ID)
	}
	ms.Unlock()

	if has {
		p.Close()
	}
}

// SendTo sends a message to the observer
func (ms *FormulatorNodeMesh) SendTo(ID string, m interface{}) error {
	ms.Lock()
	p, has := ms.peerMap[ID]
	ms.Unlock()
	if !has {
		return ErrNotExistObserverPeer
	}

	if err := p.Send(m); err != nil {
		rlog.Println(err)
		ms.RemovePeer(p.ID())
	}
	return nil
}

// SendRawTo sends a packet to the observer
func (ms *FormulatorNodeMesh) SendRawTo(ID string, bs []byte) error {
	ms.Lock()
	p, has := ms.peerMap[ID]
	ms.Unlock()
	if !has {
		return ErrNotExistObserverPeer
	}

	if err := p.SendRaw(bs); err != nil {
		rlog.Println(err)
		ms.RemovePeer(p.ID())
	}
	return nil
}

// SendRawAnyoneExcept sends a raw to any observer except the observer
func (ms *FormulatorNodeMesh) SendRawAnyoneExcept(ID string, m interface{}) error {
	var p peer.Peer
	ms.Lock()
	for k, v := range ms.peerMap {
		if ID != k {
			p = v
		}
	}
	ms.Unlock()
	if p == nil {
		return ErrNotExistObserverPeer
	}

	if err := p.Send(m); err != nil {
		rlog.Println(err)
		ms.RemovePeer(p.ID())
	}
	return nil
}

// BroadcastMessage sends a message to all peers
func (ms *FormulatorNodeMesh) BroadcastMessage(m interface{}) error {
	data, err := p2p.MessageToPacket(m)
	if err != nil {
		return err
	}

	peerMap := map[string]peer.Peer{}
	ms.Lock()
	for _, p := range ms.peerMap {
		peerMap[p.ID()] = p
	}
	ms.Unlock()

	for _, p := range peerMap {
		p.SendRaw(data)
	}
	return nil
}

func (ms *FormulatorNodeMesh) client(Address string, TargetPubHash common.PublicHash) error {
	/*
		conn, _, err := websocket.DefaultDialer.Dial(Address, nil)
		if err != nil {
			return err
		}
		defer conn.Close()
	*/
	conn, err := net.DialTimeout("tcp", Address, 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := ms.recvHandshake(conn); err != nil {
		rlog.Println("[recvHandshake]", err)
		return err
	}
	pubhash, err := ms.sendHandshake(conn)
	if err != nil {
		rlog.Println("[sendHandshake]", err)
		return err
	}
	if pubhash != TargetPubHash {
		return common.ErrInvalidPublicHash
	}
	if _, has := ms.netAddressMap[pubhash]; !has {
		return ErrInvalidObserverKey
	}

	ID := string(pubhash[:])
	//p := p2p.NewWebsocketPeer(conn, ID, pubhash.String(), time.Now().UnixNano())
	p := p2p.NewTCPPeer(conn, ID, pubhash.String(), time.Now().UnixNano())
	ms.RemovePeer(ID)
	ms.Lock()
	ms.peerMap[ID] = p
	ms.Unlock()
	defer ms.RemovePeer(p.ID())

	if err := ms.handleConnection(p); err != nil {
		rlog.Println("[handleConnection]", err)
	}
	return nil
}

func (ms *FormulatorNodeMesh) handleConnection(p peer.Peer) error {
	if debug.DEBUG {
		rlog.Println("Formulator", common.NewPublicHash(ms.key.PublicKey()).String(), "Observer Connected", p.Name())
	}

	ms.fr.OnObserverConnected(p)
	defer ms.fr.OnObserverDisconnected(p)

	for {
		m, _, err := p.ReadMessageData()
		if err != nil {
			return err
		}
		if err := ms.fr.onObserverRecv(p.ID(), m); err != nil {
			return err
		}
	}
}

func (ms *FormulatorNodeMesh) recvHandshake(conn net.Conn) error {
	//rlog.Println("recvHandshake")
	req := make([]byte, 40)
	if _, err := p2p.FillBytes(conn, req); err != nil {
		return err
	}
	ChainID := req[0]
	if ChainID != ms.fr.cs.cn.Provider().ChainID() {
		return chain.ErrInvalidChainID
	}
	timestamp := binary.LittleEndian.Uint64(req[32:])
	diff := time.Duration(uint64(time.Now().UnixNano()) - timestamp)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second*30 {
		return p2p.ErrInvalidHandshake
	}
	//rlog.Println("sendHandshakeAck")
	if sig, err := ms.key.Sign(hash.Hash(req)); err != nil {
		return err
	} else if _, err := conn.Write(sig[:]); err != nil {
		return err
	}
	return nil
}

func (ms *FormulatorNodeMesh) sendHandshake(conn net.Conn) (common.PublicHash, error) {
	//rlog.Println("sendHandshake")
	req := make([]byte, 40+common.AddressSize)
	if _, err := crand.Read(req[:32]); err != nil {
		return common.PublicHash{}, err
	}
	req[0] = ms.fr.cs.cn.Provider().ChainID()
	binary.LittleEndian.PutUint64(req[32:], uint64(time.Now().UnixNano()))
	copy(req[40:], ms.fr.Config.Formulator[:])
	if _, err := conn.Write(req); err != nil {
		return common.PublicHash{}, err
	}
	//rlog.Println("recvHandshakeAsk")
	var sig common.Signature
	if _, err := p2p.FillBytes(conn, sig[:]); err != nil {
		return common.PublicHash{}, err
	}
	pubkey, err := common.RecoverPubkey(hash.Hash(req), sig)
	if err != nil {
		return common.PublicHash{}, err
	}
	pubhash := common.NewPublicHash(pubkey)
	return pubhash, nil
}

/*
func (ms *FormulatorNodeMesh) recvHandshake(conn *websocket.Conn) error {
	//rlog.Println("recvHandshake")
	_, req, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if len(req) != 40 {
		return p2p.ErrInvalidHandshake
	}
	ChainID := req[0]
	if ChainID != ms.fr.cs.cn.Provider().ChainID() {
		return chain.ErrInvalidChainID
	}
	timestamp := binary.LittleEndian.Uint64(req[32:])
	diff := time.Duration(uint64(time.Now().UnixNano()) - timestamp)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second*30 {
		return p2p.ErrInvalidHandshake
	}
	//rlog.Println("sendHandshakeAck")
	if sig, err := ms.key.Sign(hash.Hash(req)); err != nil {
		return err
	} else if err := conn.WriteMessage(websocket.BinaryMessage, sig[:]); err != nil {
		return err
	}
	return nil
}

func (ms *FormulatorNodeMesh) sendHandshake(conn *websocket.Conn) (common.PublicHash, error) {
	//rlog.Println("sendHandshake")
	req := make([]byte, 40+common.AddressSize)
	if _, err := crand.Read(req[:32]); err != nil {
		return common.PublicHash{}, err
	}
	req[0] = ms.fr.cs.cn.Provider().ChainID()
	binary.LittleEndian.PutUint64(req[32:], uint64(time.Now().UnixNano()))
	copy(req[40:], ms.fr.Config.Formulator[:])
	if err := conn.WriteMessage(websocket.BinaryMessage, req); err != nil {
		return common.PublicHash{}, err
	}
	//rlog.Println("recvHandshakeAsk")
	_, bs, err := conn.ReadMessage()
	if err != nil {
		return common.PublicHash{}, err
	}
	if len(bs) != common.SignatureSize {
		return common.PublicHash{}, p2p.ErrInvalidHandshake
	}
	var sig common.Signature
	copy(sig[:], bs)
	pubkey, err := common.RecoverPubkey(hash.Hash(req), sig)
	if err != nil {
		return common.PublicHash{}, err
	}
	pubhash := common.NewPublicHash(pubkey)
	return pubhash, nil
}
*/
