package p2p

import (
	"bytes"
	"log"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-tron/base58/base58"

	"github.com/bluele/gcache"

	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/hash"
	"github.com/fletaio/fleta_testnet/common/key"
	"github.com/fletaio/fleta_testnet/common/queue"
	"github.com/fletaio/fleta_testnet/common/rlog"
	"github.com/fletaio/fleta_testnet/core/chain"
	"github.com/fletaio/fleta_testnet/core/txpool"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/encoding"
	"github.com/fletaio/fleta_testnet/service/p2p/peer"
)

// Node receives a block by the consensus
type Node struct {
	sync.Mutex
	key          key.Key
	ms           *NodeMesh
	cn           *chain.Chain
	statusLock   sync.Mutex
	myPublicHash common.PublicHash
	requestTimer *RequestTimer
	requestLock  sync.RWMutex
	blockQ       *queue.SortedQueue
	txMsgChans   []*chan *TxMsgItem
	txMsgIdx     uint64
	statusMap    map[string]*Status
	txpool       *txpool.TransactionPool
	txQ          *queue.ExpireQueue
	recvQueues   []*queue.Queue
	recvQCond    *sync.Cond
	sendQueues   []*queue.Queue
	sendQCond    *sync.Cond
	isRunning    bool
	closeLock    sync.RWMutex
	isClose      bool
	cache        gcache.Cache
}

// NewNode returns a Node
func NewNode(key key.Key, SeedNodeMap map[common.PublicHash]string, cn *chain.Chain, peerStorePath string) *Node {
	nd := &Node{
		key:          key,
		cn:           cn,
		myPublicHash: common.NewPublicHash(key.PublicKey()),
		blockQ:       queue.NewSortedQueue(),
		statusMap:    map[string]*Status{},
		txpool:       txpool.NewTransactionPool(),
		txQ:          queue.NewExpireQueue(),
		recvQueues: []*queue.Queue{
			queue.NewQueue(), //block
			queue.NewQueue(), //tx
			queue.NewQueue(), //peer
		},
		sendQueues: []*queue.Queue{
			queue.NewQueue(), //block
			queue.NewQueue(), //tx
			queue.NewQueue(), //peer
		},
		cache: gcache.New(500).LRU().Build(),
	}
	nd.ms = NewNodeMesh(cn.Provider().ChainID(), key, SeedNodeMap, nd, peerStorePath)
	nd.requestTimer = NewRequestTimer(nd)
	nd.txQ.AddGroup(60 * time.Second)
	nd.txQ.AddGroup(600 * time.Second)
	nd.txQ.AddGroup(3600 * time.Second)
	nd.txQ.AddHandler(nd)
	rlog.SetRLogAddress("nd:" + nd.myPublicHash.String())
	return nd
}

// Init initializes node
func (nd *Node) Init() error {
	fc := encoding.Factory("message")
	fc.Register(types.DefineHashedType("p2p.PingMessage"), &PingMessage{})
	fc.Register(types.DefineHashedType("p2p.StatusMessage"), &StatusMessage{})
	fc.Register(types.DefineHashedType("p2p.RequestMessage"), &RequestMessage{})
	fc.Register(types.DefineHashedType("p2p.BlockMessage"), &BlockMessage{})
	fc.Register(types.DefineHashedType("p2p.TransactionMessage"), &TransactionMessage{})
	fc.Register(types.DefineHashedType("p2p.PeerListMessage"), &PeerListMessage{})
	fc.Register(types.DefineHashedType("p2p.RequestPeerListMessage"), &RequestPeerListMessage{})
	return nil
}

// Close terminates the node
func (nd *Node) Close() {
	nd.closeLock.Lock()
	defer nd.closeLock.Unlock()

	nd.Lock()
	defer nd.Unlock()

	nd.isClose = true
	nd.cn.Close()
}

// OnItemExpired is called when the item is expired
func (nd *Node) OnItemExpired(Interval time.Duration, Key string, Item interface{}, IsLast bool) {
	msg := Item.(*TransactionMessage)
	nd.limitCastMessage(1, msg)
	if IsLast {
		var TxHash hash.Hash256
		copy(TxHash[:], []byte(Key))
		nd.txpool.Remove(TxHash, msg.Tx)
	}
}

// Run starts the node
func (nd *Node) Run(BindAddress string) {
	nd.Lock()
	if nd.isRunning {
		nd.Unlock()
		return
	}
	nd.isRunning = true
	nd.Unlock()

	go nd.ms.Run(BindAddress)
	go nd.requestTimer.Run()

	WorkerCount := runtime.NumCPU() - 1
	if WorkerCount < 1 {
		WorkerCount = 1
	}
	workerEnd := make([]*chan struct{}, WorkerCount)
	nd.txMsgChans = make([]*chan *TxMsgItem, WorkerCount)
	for i := 0; i < WorkerCount; i++ {
		mch := make(chan *TxMsgItem)
		nd.txMsgChans[i] = &mch
		ch := make(chan struct{})
		workerEnd[i] = &ch
		go func(pMsgCh *chan *TxMsgItem, pEndCh *chan struct{}) {
			for !nd.isClose {
				select {
				case item := <-(*pMsgCh):
					if err := nd.addTx(item.Message.TxType, item.Message.Tx, item.Message.Sigs); err != nil {
						//rlog.Println("TransactionError", chain.HashTransactionByType(nd.cn.Provider().ChainID(), item.Message.TxType, item.Message.Tx).String(), err.Error())
						(*item.ErrCh) <- err
						break
					}
					//rlog.Println("TransactionAppended", chain.HashTransactionByType(nd.cn.Provider().ChainID(), item.Message.TxType, item.Message.Tx).String())
					(*item.ErrCh) <- nil

					var SenderPublicHash common.PublicHash
					copy(SenderPublicHash[:], []byte(item.PeerID))
					nd.exceptLimitCastMessage(1, SenderPublicHash, item.Message)
				case <-(*pEndCh):
					return
				}
			}
		}(&mch, &ch)
	}

	go func() {
		for !nd.isClose {
			hasMessage := false
			for !nd.isClose {
				for _, q := range nd.recvQueues {
					v := q.Pop()
					if v == nil {
						continue
					}
					hasMessage = true
					item := v.(*RecvMessageItem)
					if _, is := item.Message.(*TransactionMessage); !is {
						switch msg := item.Message.(type) {
						case *RequestMessage:
							log.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
						case *StatusMessage:
							log.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
						case *BlockMessage:
							log.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Block.Header.Height)
						default:
							log.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name())
						}
					}
					if err := nd.handlePeerMessage(item.PeerID, item.Message); err != nil {
						nd.ms.RemovePeer(item.PeerID)
					}
					break
				}
				if !hasMessage {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for !nd.isClose {
			hasMessage := false
			for !nd.isClose {
				for _, q := range nd.sendQueues {
					v := q.Pop()
					if v == nil {
						continue
					}
					hasMessage = true
					item := v.(*SendMessageItem)
					if len(item.Packet) > 0 {
						log.Println("SendMessage", item.Target, item.Limit, "BlockMessage", item.Height)
						if err := nd.ms.SendRawTo(item.Target, item.Packet); err != nil {
							nd.ms.RemovePeer(string(item.Target[:]))
						}
					} else {
						if _, is := item.Message.(*TransactionMessage); !is {
							switch msg := item.Message.(type) {
							case *RequestMessage:
								log.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
							case *StatusMessage:
								log.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
							case *BlockMessage:
								log.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Block.Header.Height)
							default:
								log.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name())
							}
						}
						var EmptyHash common.PublicHash
						if bytes.Equal(item.Target[:], EmptyHash[:]) {
							if item.Limit > 0 {
								if err := nd.ms.ExceptCastLimit("", item.Message, item.Limit); err != nil {
									nd.ms.RemovePeer(string(item.Target[:]))
								}
							} else {
								if err := nd.ms.BroadcastMessage(item.Message); err != nil {
									nd.ms.RemovePeer(string(item.Target[:]))
								}
							}
						} else {
							if item.Limit > 0 {
								if err := nd.ms.ExceptCastLimit(string(item.Target[:]), item.Message, item.Limit); err != nil {
									nd.ms.RemovePeer(string(item.Target[:]))
								}
							} else {
								if err := nd.ms.SendTo(item.Target, item.Message); err != nil {
									nd.ms.RemovePeer(string(item.Target[:]))
								}
							}
						}
					}
					break
				}
				if !hasMessage {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	blockTimer := time.NewTimer(time.Millisecond)
	blockRequestTimer := time.NewTimer(time.Millisecond)
	for !nd.isClose {
		select {
		case <-blockTimer.C:
			nd.Lock()
			hasItem := false
			TargetHeight := uint64(nd.cn.Provider().Height() + 1)
			Count := 0
			item := nd.blockQ.PopUntil(TargetHeight)
			for item != nil {
				b := item.(*types.Block)
				if err := nd.cn.ConnectBlock(b); err != nil {
					rlog.Println(err)
					panic(err)
					break
				}
				rlog.Println("Node", nd.myPublicHash.String(), nd.cn.Provider().Height(), "BlockConnected", b.Header.Generator.String(), b.Header.Height)
				TargetHeight++
				Count++
				if Count > 100 {
					break
				}
				item = nd.blockQ.PopUntil(TargetHeight)
				hasItem = true
			}
			nd.Unlock()

			if hasItem {
				nd.broadcastStatus()
				nd.tryRequestBlocks()
			}

			blockTimer.Reset(50 * time.Millisecond)
		case <-blockRequestTimer.C:
			nd.tryRequestBlocks()
			blockRequestTimer.Reset(500 * time.Millisecond)
		}
	}
}

// OnTimerExpired called when rquest expired
func (nd *Node) OnTimerExpired(height uint32, value string) {
	nd.tryRequestBlocks()
}

// OnConnected called when peer connected
func (nd *Node) OnConnected(p peer.Peer) {
	nd.statusLock.Lock()
	nd.statusMap[p.ID()] = &Status{}
	nd.statusLock.Unlock()

	var SenderPublicHash common.PublicHash
	copy(SenderPublicHash[:], []byte(p.ID()))
	nd.sendStatusTo(SenderPublicHash)
}

// OnDisconnected called when peer disconnected
func (nd *Node) OnDisconnected(p peer.Peer) {
	nd.statusLock.Lock()
	delete(nd.statusMap, p.ID())
	nd.statusLock.Unlock()

	nd.requestTimer.RemovesByValue(p.ID())
	go nd.tryRequestBlocks()
}

// OnRecv called when message received
func (nd *Node) OnRecv(ID string, m interface{}) error {
	item := &RecvMessageItem{
		PeerID:  ID,
		Message: m,
	}
	switch m.(type) {
	case *RequestMessage:
		nd.recvQueues[0].Push(item)
	case *StatusMessage:
		nd.recvQueues[0].Push(item)
	case *BlockMessage:
		nd.recvQueues[0].Push(item)
	case *TransactionMessage:
		nd.recvQueues[1].Push(item)
	case *PeerListMessage:
		nd.recvQueues[2].Push(item)
	case *RequestPeerListMessage:
		nd.recvQueues[2].Push(item)
	}
	return nil
}

func (nd *Node) sendMessage(Priority int, Target common.PublicHash, m interface{}) {
	nd.sendQueues[Priority].Push(&SendMessageItem{
		Target:  Target,
		Message: m,
	})
}

func (nd *Node) sendMessagePacket(Priority int, Target common.PublicHash, raw []byte, Height uint32) {
	nd.sendQueues[Priority].Push(&SendMessageItem{
		Target: Target,
		Packet: raw,
		Height: Height,
	})
}

func (nd *Node) broadcastMessage(Priority int, m interface{}) {
	nd.sendQueues[Priority].Push(&SendMessageItem{
		Message: m,
	})
}

func (nd *Node) limitCastMessage(Priority int, m interface{}) {
	nd.sendQueues[Priority].Push(&SendMessageItem{
		Message: m,
		Limit:   3,
	})
}

func (nd *Node) exceptLimitCastMessage(Priority int, Target common.PublicHash, m interface{}) {
	nd.sendQueues[Priority].Push(&SendMessageItem{
		Target:  Target,
		Message: m,
		Limit:   3,
	})
}

func (nd *Node) handlePeerMessage(ID string, m interface{}) error {
	var SenderPublicHash common.PublicHash
	copy(SenderPublicHash[:], []byte(ID))

	switch msg := m.(type) {
	case *RequestMessage:
		cp := nd.cn.Provider()
		Height := cp.Height()
		if msg.Height > Height {
			return nil
		}
		var raw []byte
		value, err := nd.cache.Get(msg.Height)
		if err != nil {
			b, err := cp.Block(msg.Height)
			if err != nil {
				return err
			}
			data, err := MessageToPacket(&BlockMessage{
				Block: b,
			})
			if err != nil {
				return err
			}
			nd.cache.Set(msg.Height, data)
			raw = data
		} else {
			raw = value.([]byte)
		}
		nd.sendMessagePacket(0, SenderPublicHash, raw, msg.Height)
		return nil
	case *StatusMessage:
		nd.statusLock.Lock()
		if status, has := nd.statusMap[ID]; has {
			if status.Height < msg.Height {
				status.Version = msg.Version
				status.Height = msg.Height
				status.LastHash = msg.LastHash
			}
		}
		nd.statusLock.Unlock()

		Height := nd.cn.Provider().Height()
		if Height < msg.Height {
			for q := uint32(0); q < 3; q++ {
				BaseHeight := Height + q*10
				if BaseHeight > msg.Height {
					break
				}
				for i := BaseHeight + 1; i <= BaseHeight+10 && i <= msg.Height; i++ {
					if !nd.requestTimer.Exist(i) {
						if nd.blockQ.Find(uint64(i)) == nil {
							nd.sendRequestBlockTo(SenderPublicHash, i)
						}
					}
				}
			}
		} else {
			h, err := nd.cn.Provider().Hash(msg.Height)
			if err != nil {
				return err
			}
			if h != msg.LastHash {
				//TODO : critical error signal
				rlog.Println(chain.ErrFoundForkedBlock, ID, h.String(), msg.LastHash.String(), msg.Height)
				nd.ms.RemovePeer(ID)
			}
		}
		return nil
	case *BlockMessage:
		if err := nd.addBlock(msg.Block); err != nil {
			if err == chain.ErrFoundForkedBlock {
				//TODO : critical error signal
				nd.ms.RemovePeer(ID)
			}
			return err
		}
		nd.statusLock.Lock()
		if status, has := nd.statusMap[ID]; has {
			lastHeight := msg.Block.Header.Height
			if status.Height < lastHeight {
				status.Height = lastHeight
			}
		}
		nd.statusLock.Unlock()
		return nil
	case *TransactionMessage:
		errCh := make(chan error)
		idx := atomic.AddUint64(&nd.txMsgIdx, 1) % uint64(len(nd.txMsgChans))
		(*nd.txMsgChans[idx]) <- &TxMsgItem{
			Message: msg,
			PeerID:  ID,
			ErrCh:   &errCh,
		}
		err := <-errCh
		if err != ErrInvalidUTXO && err != txpool.ErrExistTransaction && err != txpool.ErrTooFarSeq && err != txpool.ErrPastSeq {
			return err
		}
		return nil
	case *PeerListMessage:
		nd.ms.AddPeerList(msg.Ips, msg.Hashs)
		return nil
	case *RequestPeerListMessage:
		ips, hashs := nd.ms.nodePoolManager.GetPeerList()
		nd.sendMessage(2, SenderPublicHash, &PeerListMessage{
			Ips:   ips,
			Hashs: hashs,
		})
		return nil
	default:
		panic(ErrUnknownMessage) //TEMP
		return ErrUnknownMessage
	}
	return nil
}

func (nd *Node) addBlock(b *types.Block) error {
	cp := nd.cn.Provider()
	if b.Header.Height <= cp.Height() {
		h, err := cp.Hash(b.Header.Height)
		if err != nil {
			return err
		}
		if h != encoding.Hash(b.Header) {
			//TODO : critical error signal
			return chain.ErrFoundForkedBlock
		}
	} else {
		if item := nd.blockQ.FindOrInsert(b, uint64(b.Header.Height)); item != nil {
			old := item.(*types.Block)
			if encoding.Hash(old.Header) != encoding.Hash(b.Header) {
				//TODO : critical error signal
				return chain.ErrFoundForkedBlock
			}
		}
	}
	return nil
}

// AddTx adds tx to txpool that only have valid signatures
func (nd *Node) AddTx(tx types.Transaction, sigs []common.Signature) error {
	fc := encoding.Factory("transaction")
	t, err := fc.TypeOf(tx)
	if err != nil {
		return err
	}
	if err := nd.addTx(t, tx, sigs); err != nil {
		return err
	}
	nd.limitCastMessage(1, &TransactionMessage{
		TxType: t,
		Tx:     tx,
		Sigs:   sigs,
	})
	return nil
}

func (nd *Node) addTx(t uint16, tx types.Transaction, sigs []common.Signature) error {
	if nd.txpool.Size() > 65535 {
		return txpool.ErrTransactionPoolOverflowed
	}

	TxHash := chain.HashTransactionByType(nd.cn.Provider().ChainID(), t, tx)

	ctx := nd.cn.NewContext()
	if nd.txpool.IsExist(TxHash) {
		return txpool.ErrExistTransaction
	}
	if atx, is := tx.(chain.AccountTransaction); is {
		seq := ctx.Seq(atx.From())
		if atx.Seq() <= seq {
			return txpool.ErrPastSeq
		} else if atx.Seq() > seq+100 {
			return txpool.ErrTooFarSeq
		}
	}
	signers := make([]common.PublicHash, 0, len(sigs))
	for _, sig := range sigs {
		pubkey, err := common.RecoverPubkey(TxHash, sig)
		if err != nil {
			return err
		}
		signers = append(signers, common.NewPublicHash(pubkey))
	}
	pid := uint8(t >> 8)
	p, err := nd.cn.Process(pid)
	if err != nil {
		return err
	}
	ctw := types.NewContextWrapper(pid, ctx)
	if err := tx.Validate(p, ctw, signers); err != nil {
		return err
	}
	if err := nd.txpool.Push(nd.cn.Provider().ChainID(), t, TxHash, tx, sigs, signers); err != nil {
		return err
	}
	nd.txQ.Push(string(TxHash[:]), &TransactionMessage{
		TxType: t,
		Tx:     tx,
		Sigs:   sigs,
	})
	return nil
}

func (nd *Node) tryRequestBlocks() {
	nd.requestLock.Lock()
	defer nd.requestLock.Unlock()

	Height := nd.cn.Provider().Height()
	for q := uint32(0); q < 3; q++ {
		BaseHeight := Height + q*10

		var LimitHeight uint32
		var selectedPubHash string
		nd.statusLock.Lock()
		for pubhash, status := range nd.statusMap {
			if BaseHeight+10 <= status.Height {
				selectedPubHash = pubhash
				LimitHeight = status.Height
				break
			}
		}
		if len(selectedPubHash) == 0 {
			for pubhash, status := range nd.statusMap {
				if BaseHeight <= status.Height {
					selectedPubHash = pubhash
					LimitHeight = status.Height
					break
				}
			}
		}
		nd.statusLock.Unlock()

		if len(selectedPubHash) == 0 {
			break
		}
		var TargetPublicHash common.PublicHash
		copy(TargetPublicHash[:], []byte(selectedPubHash))
		for i := BaseHeight + 1; i <= BaseHeight+10 && i <= LimitHeight; i++ {
			if !nd.requestTimer.Exist(i) {
				if nd.blockQ.Find(uint64(i)) == nil {
					nd.sendRequestBlockTo(TargetPublicHash, i)
				}
			}
		}
	}
}

func (nd *Node) cleanPool(b *types.Block) {
	for i, tx := range b.Transactions {
		t := b.TransactionTypes[i]
		TxHash := chain.HashTransactionByType(nd.cn.Provider().ChainID(), t, tx)
		nd.txpool.Remove(TxHash, tx)
		nd.txQ.Remove(string(TxHash[:]))
	}
}

// TxMsgItem used to store transaction message
type TxMsgItem struct {
	Message *TransactionMessage
	PeerID  string
	ErrCh   *chan error
}
