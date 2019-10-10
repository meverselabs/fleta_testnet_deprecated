package pof

import (
	"bytes"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/fletaio/fleta_testnet/common/amount"

	"github.com/fletaio/fleta_testnet/process/vault"

	"github.com/fletaio/fleta_testnet/common/debug"

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
	"github.com/fletaio/fleta_testnet/service/p2p"
	"github.com/fletaio/fleta_testnet/service/p2p/peer"
)

// FormulatorConfig defines configuration of the formulator
type FormulatorConfig struct {
	Formulator              common.Address
	MaxTransactionsPerBlock int
}

// FormulatorNode procudes a block by the consensus
type FormulatorNode struct {
	sync.Mutex
	Config               *FormulatorConfig
	cs                   *Consensus
	ms                   *FormulatorNodeMesh
	nm                   *p2p.NodeMesh
	key                  key.Key
	ndkey                key.Key
	myPublicHash         common.PublicHash
	statusLock           sync.Mutex
	genLock              sync.Mutex
	lastGenMessages      []*BlockGenMessage
	lastObSignMessageMap map[uint32]*BlockObSignMessage
	lastContextes        []*types.Context
	lastReqMessage       *BlockReqMessage
	lastGenHeight        uint32
	lastGenTime          int64
	statusMap            map[string]*p2p.Status
	obStatusMap          map[string]*p2p.Status
	requestTimer         *p2p.RequestTimer
	requestNodeTimer     *p2p.RequestTimer
	requestLock          sync.RWMutex
	blockQ               *queue.SortedQueue
	txpool               *txpool.TransactionPool
	txQ                  *queue.ExpireQueue
	txWaitQ              *queue.Queue
	recvQueues           []*queue.Queue
	recvQCond            *sync.Cond
	sendQueues           []*queue.Queue
	sendQCond            *sync.Cond
	isRunning            bool
	closeLock            sync.RWMutex
	isClose              bool
	cache                gcache.Cache
}

// NewFormulatorNode returns a FormulatorNode
func NewFormulatorNode(Config *FormulatorConfig, key key.Key, ndkey key.Key, NetAddressMap map[common.PublicHash]string, SeedNodeMap map[common.PublicHash]string, cs *Consensus, peerStorePath string) *FormulatorNode {
	if Config.MaxTransactionsPerBlock == 0 {
		Config.MaxTransactionsPerBlock = 5000
	}
	fr := &FormulatorNode{
		Config:               Config,
		cs:                   cs,
		key:                  key,
		ndkey:                ndkey,
		myPublicHash:         common.NewPublicHash(ndkey.PublicKey()),
		lastGenMessages:      []*BlockGenMessage{},
		lastObSignMessageMap: map[uint32]*BlockObSignMessage{},
		lastContextes:        []*types.Context{},
		statusMap:            map[string]*p2p.Status{},
		obStatusMap:          map[string]*p2p.Status{},
		requestTimer:         p2p.NewRequestTimer(nil),
		requestNodeTimer:     p2p.NewRequestTimer(nil),
		blockQ:               queue.NewSortedQueue(),
		txpool:               txpool.NewTransactionPool(),
		txQ:                  queue.NewExpireQueue(),
		txWaitQ:              queue.NewQueue(),
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
	fr.ms = NewFormulatorNodeMesh(key, NetAddressMap, fr)
	fr.nm = p2p.NewNodeMesh(fr.cs.cn.Provider().ChainID(), ndkey, SeedNodeMap, fr, peerStorePath)
	fr.txQ.AddGroup(60 * time.Second)
	fr.txQ.AddGroup(600 * time.Second)
	fr.txQ.AddGroup(3600 * time.Second)
	fr.txQ.AddHandler(fr)
	rlog.SetRLogAddress("fr:" + Config.Formulator.String())
	return fr
}

// Close terminates the formulator
func (fr *FormulatorNode) Close() {
	fr.closeLock.Lock()
	defer fr.closeLock.Unlock()

	fr.Lock()
	defer fr.Unlock()

	fr.isClose = true
	fr.cs.cn.Close()
}

// Init initializes formulator
func (fr *FormulatorNode) Init() error {
	fc := encoding.Factory("message")
	fc.Register(types.DefineHashedType("pof.BlockReqMessage"), &BlockReqMessage{})
	fc.Register(types.DefineHashedType("pof.BlockGenMessage"), &BlockGenMessage{})
	fc.Register(types.DefineHashedType("pof.BlockObSignMessage"), &BlockObSignMessage{})
	fc.Register(types.DefineHashedType("p2p.PingMessage"), &p2p.PingMessage{})
	fc.Register(types.DefineHashedType("p2p.StatusMessage"), &p2p.StatusMessage{})
	fc.Register(types.DefineHashedType("p2p.BlockMessage"), &p2p.BlockMessage{})
	fc.Register(types.DefineHashedType("p2p.RequestMessage"), &p2p.RequestMessage{})
	fc.Register(types.DefineHashedType("p2p.TransactionMessage"), &p2p.TransactionMessage{})
	fc.Register(types.DefineHashedType("p2p.PeerListMessage"), &p2p.PeerListMessage{})
	fc.Register(types.DefineHashedType("p2p.RequestPeerListMessage"), &p2p.RequestPeerListMessage{})
	return nil
}

// Run runs the formulator
func (fr *FormulatorNode) Run(BindAddress string) {
	fr.Lock()
	if fr.isRunning {
		fr.Unlock()
		return
	}
	fr.isRunning = true
	fr.Unlock()

	go fr.ms.Run()
	go fr.nm.Run(BindAddress)
	go fr.requestTimer.Run()
	go fr.requestNodeTimer.Run()

	WorkerCount := runtime.NumCPU() - 1
	if WorkerCount < 1 {
		WorkerCount = 1
	}
	for i := 0; i < WorkerCount; i++ {
		go func() {
			for !fr.isClose {
				Count := 0
				for !fr.isClose {
					v := fr.txWaitQ.Pop()
					if v == nil {
						break
					}
					item := v.(*p2p.TxMsgItem)
					p := debug.Start("Run.addTx")
					if err := fr.addTx(item.Message.TxType, item.Message.Tx, item.Message.Sigs); err != nil {
						if err != p2p.ErrInvalidUTXO && err != txpool.ErrExistTransaction && err != txpool.ErrTooFarSeq && err != txpool.ErrPastSeq {
							rlog.Println("TransactionError", chain.HashTransactionByType(fr.cs.cn.Provider().ChainID(), item.Message.TxType, item.Message.Tx).String(), err.Error())
							if len(item.PeerID) > 0 {
								fr.nm.RemovePeer(item.PeerID)
							}
						}
					}
					rlog.Println("TransactionAppended", chain.HashTransactionByType(fr.cs.cn.Provider().ChainID(), item.Message.TxType, item.Message.Tx).String())

					if len(item.PeerID) > 0 {
						var SenderPublicHash common.PublicHash
						copy(SenderPublicHash[:], []byte(item.PeerID))
						fr.exceptLimitCastMessage(1, SenderPublicHash, item.Message)
					} else {
						fr.limitCastMessage(1, item.Message)
					}
					p.Stop()

					Count++
					if Count > 500 {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}

	go func() {
		for !fr.isClose {
			time.Sleep(30 * time.Second)
			debug.Result()
			log.Println("------------------------------")
		}
	}()

	go func() {
		for !fr.isClose {
			hasMessage := false
			for !fr.isClose {
				for i, rq := range fr.recvQueues {
					if v := rq.Pop(); v != nil {
						hasMessage = true
						item := v.(*p2p.RecvMessageItem)
						if _, is := item.Message.(*p2p.TransactionMessage); !is {
							switch msg := item.Message.(type) {
							case *p2p.RequestMessage:
								rlog.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
							case *p2p.StatusMessage:
								rlog.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
							case *p2p.BlockMessage:
								rlog.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Block.Header.Height)
							default:
								rlog.Println("RecvMessage", base58.Encode([]byte(item.PeerID[:])), reflect.ValueOf(item.Message).Elem().Type().Name())
							}
						}
						p := debug.Start(reflect.ValueOf(item.Message).Elem().Type().Name() + ".Recv")
						if err := fr.handlePeerMessage(item.PeerID, item.Message); err != nil {
							fr.nm.RemovePeer(item.PeerID)
						}
						p.Stop()
					}
					sq := fr.sendQueues[i]
					if v := sq.Pop(); v != nil {
						hasMessage = true
						item := v.(*p2p.SendMessageItem)
						if len(item.Packet) > 0 {
							p := debug.Start("BlockMessage.Send")
							rlog.Println("SendMessage", item.Target, item.Limit, "BlockMessage", item.Height)
							if err := fr.nm.SendRawTo(item.Target, item.Packet); err != nil {
								fr.nm.RemovePeer(string(item.Target[:]))
							}
							p.Stop()
						} else {
							p := debug.Start(reflect.ValueOf(item.Message).Elem().Type().Name() + ".Send")
							if _, is := item.Message.(*p2p.TransactionMessage); !is {
								switch msg := item.Message.(type) {
								case *p2p.RequestMessage:
									rlog.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
								case *p2p.StatusMessage:
									rlog.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Height)
								case *p2p.BlockMessage:
									rlog.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name(), msg.Block.Header.Height)
								default:
									rlog.Println("SendMessage", item.Target, item.Limit, reflect.ValueOf(item.Message).Elem().Type().Name())
								}
							}
							var EmptyHash common.PublicHash
							if bytes.Equal(item.Target[:], EmptyHash[:]) {
								if item.Limit > 0 {
									if err := fr.nm.ExceptCastLimit("", item.Message, item.Limit); err != nil {
										fr.nm.RemovePeer(string(item.Target[:]))
									}
								} else {
									if err := fr.nm.BroadcastMessage(item.Message); err != nil {
										fr.nm.RemovePeer(string(item.Target[:]))
									}
								}
							} else {
								if item.Limit > 0 {
									if err := fr.nm.ExceptCastLimit(string(item.Target[:]), item.Message, item.Limit); err != nil {
										fr.nm.RemovePeer(string(item.Target[:]))
									}
								} else {
									if err := fr.nm.SendTo(item.Target, item.Message); err != nil {
										fr.nm.RemovePeer(string(item.Target[:]))
									}
								}
							}
							p.Stop()
						}
					}
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
	for !fr.isClose {
		select {
		case <-blockTimer.C:
			fr.Lock()
			hasItem := false
			TargetHeight := uint64(fr.cs.cn.Provider().Height() + 1)
			Count := 0
			item := fr.blockQ.PopUntil(TargetHeight)
			for item != nil {
				b := item.(*types.Block)
				if err := fr.cs.cn.ConnectBlock(b); err != nil {
					break
				}
				fr.cleanPool(b)
				rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockConnected", b.Header.Generator.String(), b.Header.Height, len(b.Transactions))
				TargetHeight++
				Count++
				if Count > 10 {
					break
				}
				item = fr.blockQ.PopUntil(TargetHeight)
				hasItem = true
			}
			fr.Unlock()

			if hasItem {
				fr.broadcastStatus()
			}

			blockTimer.Reset(50 * time.Millisecond)
		case <-blockRequestTimer.C:
			fr.tryRequestBlocks()
			fr.tryRequestNext()
			blockRequestTimer.Reset(500 * time.Millisecond)
		}
	}
}

// AddTx adds tx to txpool that only have valid signatures
func (fr *FormulatorNode) AddTx(tx types.Transaction, sigs []common.Signature) error {
	fc := encoding.Factory("transaction")
	t, err := fc.TypeOf(tx)
	if err != nil {
		return err
	}
	fr.txWaitQ.Push(&p2p.TxMsgItem{
		Message: &p2p.TransactionMessage{
			TxType: t,
			Tx:     tx,
			Sigs:   sigs,
		},
	})
	return nil
}

func (fr *FormulatorNode) addTx(t uint16, tx types.Transaction, sigs []common.Signature) error {
	if fr.txpool.Size() > 65535 {
		return txpool.ErrTransactionPoolOverflowed
	}

	TxHash := chain.HashTransactionByType(fr.cs.cn.Provider().ChainID(), t, tx)

	if fr.txpool.IsExist(TxHash) {
		return txpool.ErrExistTransaction
	}
	cp := fr.cs.cn.Provider()
	if atx, is := tx.(chain.AccountTransaction); is {
		seq := cp.Seq(atx.From())
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
	p, err := fr.cs.cn.Process(pid)
	if err != nil {
		return err
	}
	ctx := fr.cs.ct.NewContext()
	ctw := types.NewContextWrapper(pid, ctx)
	if err := tx.Validate(p, ctw, signers); err != nil {
		return err
	}
	if err := fr.txpool.Push(fr.cs.cn.Provider().ChainID(), t, TxHash, tx, sigs, signers); err != nil {
		return err
	}
	fr.txQ.Push(string(TxHash[:]), &p2p.TransactionMessage{
		TxType: t,
		Tx:     tx,
		Sigs:   sigs,
	})
	return nil
}

// OnTimerExpired called when rquest expired
func (fr *FormulatorNode) OnTimerExpired(height uint32, value string) {
	go fr.tryRequestBlocks()
}

// OnItemExpired is called when the item is expired
func (fr *FormulatorNode) OnItemExpired(Interval time.Duration, Key string, Item interface{}, IsLast bool) {
	msg := Item.(*p2p.TransactionMessage)
	fr.limitCastMessage(1, msg)
	if IsLast {
		var TxHash hash.Hash256
		copy(TxHash[:], []byte(Key))
		fr.txpool.Remove(TxHash, msg.Tx)
	}
}

// OnObserverConnected is called after a new observer peer is connected
func (fr *FormulatorNode) OnObserverConnected(p peer.Peer) {
	fr.statusLock.Lock()
	fr.obStatusMap[p.ID()] = &p2p.Status{}
	fr.statusLock.Unlock()

	cp := fr.cs.cn.Provider()
	height, lastHash, _ := cp.LastStatus()
	nm := &p2p.StatusMessage{
		Version:  cp.Version(),
		Height:   height,
		LastHash: lastHash,
	}
	p.Send(nm)
}

// OnObserverDisconnected is called when the observer peer is disconnected
func (fr *FormulatorNode) OnObserverDisconnected(p peer.Peer) {
	fr.statusLock.Lock()
	delete(fr.obStatusMap, p.ID())
	fr.statusLock.Unlock()
	fr.requestTimer.RemovesByValue(p.ID())
	go fr.tryRequestNext()
}

// OnConnected is called after a new  peer is connected
func (fr *FormulatorNode) OnConnected(p peer.Peer) {
	fr.statusLock.Lock()
	fr.statusMap[p.ID()] = &p2p.Status{}
	fr.statusLock.Unlock()

	var SenderPublicHash common.PublicHash
	copy(SenderPublicHash[:], []byte(p.ID()))
	fr.sendStatusTo(SenderPublicHash)
}

// OnDisconnected is called when the  peer is disconnected
func (fr *FormulatorNode) OnDisconnected(p peer.Peer) {
	fr.statusLock.Lock()
	delete(fr.statusMap, p.ID())
	fr.statusLock.Unlock()
	fr.requestNodeTimer.RemovesByValue(p.ID())
	go fr.tryRequestBlocks()
}

func (fr *FormulatorNode) onObserverRecv(ID string, m interface{}) error {
	rlog.Println("ObRecvMessage", base58.Encode([]byte(ID[:])), reflect.ValueOf(m).Elem().Type().Name())

	if err := fr.handleObserverMessage(ID, m, 0); err != nil {
		return err
	}
	return nil
}

// OnRecv called when message received
func (fr *FormulatorNode) OnRecv(ID string, m interface{}) error {
	item := &p2p.RecvMessageItem{
		PeerID:  ID,
		Message: m,
	}
	switch m.(type) {
	case *p2p.RequestMessage:
		fr.recvQueues[0].Push(item)
	case *p2p.StatusMessage:
		fr.recvQueues[0].Push(item)
	case *p2p.BlockMessage:
		fr.recvQueues[0].Push(item)
	case *p2p.TransactionMessage:
		fr.recvQueues[1].Push(item)
	case *p2p.PeerListMessage:
		fr.recvQueues[2].Push(item)
	case *p2p.RequestPeerListMessage:
		fr.recvQueues[2].Push(item)
	}
	return nil
}

func (fr *FormulatorNode) handlePeerMessage(ID string, m interface{}) error {
	var SenderPublicHash common.PublicHash
	copy(SenderPublicHash[:], []byte(ID))

	switch msg := m.(type) {
	case *p2p.RequestMessage:
		cp := fr.cs.cn.Provider()
		Height := cp.Height()
		if msg.Height > Height {
			return nil
		}
		var raw []byte
		value, err := fr.cache.Get(msg.Height)
		if err != nil {
			b, err := cp.Block(msg.Height)
			if err != nil {
				return err
			}
			data, err := p2p.MessageToPacket(&p2p.BlockMessage{
				Block: b,
			})
			if err != nil {
				return err
			}
			fr.cache.Set(msg.Height, data)
			raw = data
		} else {
			raw = value.([]byte)
		}
		fr.sendMessagePacket(0, SenderPublicHash, raw, msg.Height)
		return nil
	case *p2p.StatusMessage:
		fr.statusLock.Lock()
		if status, has := fr.statusMap[ID]; has {
			if status.Height < msg.Height {
				status.Version = msg.Version
				status.Height = msg.Height
				status.LastHash = msg.LastHash
			}
		}
		fr.statusLock.Unlock()

		Height := fr.cs.cn.Provider().Height()
		if Height < msg.Height {
			for q := uint32(0); q < 3; q++ {
				BaseHeight := Height + q*10
				if BaseHeight > msg.Height {
					break
				}
				for i := BaseHeight + 1; i <= BaseHeight+10 && i <= msg.Height; i++ {
					if !fr.requestNodeTimer.Exist(i) {
						if fr.blockQ.Find(uint64(i)) == nil {
							fr.sendRequestBlockToNode(SenderPublicHash, i)
						}
					}
				}
			}
		} else {
			h, err := fr.cs.cn.Provider().Hash(msg.Height)
			if err != nil {
				return err
			}
			if h != msg.LastHash {
				//TODO : critical error signal
				rlog.Println(ID, h.String(), msg.LastHash.String(), msg.Height)
				fr.nm.RemovePeer(ID)
			}
		}
	case *p2p.BlockMessage:
		if err := fr.addBlock(msg.Block); err != nil {
			if err == chain.ErrFoundForkedBlock {
				panic(err)
			}
			return err
		}

		fr.statusLock.Lock()
		if status, has := fr.statusMap[ID]; has {
			lastHeight := msg.Block.Header.Height
			if status.Height < lastHeight {
				status.Height = lastHeight
			}
		}
		fr.statusLock.Unlock()
	case *p2p.TransactionMessage:
		fr.txWaitQ.Push(&p2p.TxMsgItem{
			Message: msg,
			PeerID:  ID,
		})
		return nil
	case *p2p.PeerListMessage:
		fr.nm.AddPeerList(msg.Ips, msg.Hashs)
		return nil
	case *p2p.RequestPeerListMessage:
		fr.nm.SendPeerList(ID)
		return nil
	default:
		panic(p2p.ErrUnknownMessage) //TEMP
		return p2p.ErrUnknownMessage
	}
	return nil
}

func (fr *FormulatorNode) tryRequestBlocks() {
	fr.requestLock.Lock()
	defer fr.requestLock.Unlock()

	Height := fr.cs.cn.Provider().Height()
	for q := uint32(0); q < 3; q++ {
		BaseHeight := Height + q*10

		var LimitHeight uint32
		var selectedPubHash string
		fr.statusLock.Lock()
		for pubhash, status := range fr.statusMap {
			if BaseHeight+10 <= status.Height {
				selectedPubHash = pubhash
				LimitHeight = status.Height
				break
			}
		}
		if len(selectedPubHash) == 0 {
			for pubhash, status := range fr.statusMap {
				if BaseHeight <= status.Height {
					selectedPubHash = pubhash
					LimitHeight = status.Height
					break
				}
			}
		}
		fr.statusLock.Unlock()

		if len(selectedPubHash) == 0 {
			break
		}
		var TargetPublicHash common.PublicHash
		copy(TargetPublicHash[:], []byte(selectedPubHash))
		for i := BaseHeight + 1; i <= BaseHeight+10 && i <= LimitHeight; i++ {
			if !fr.requestNodeTimer.Exist(i) {
				if fr.blockQ.Find(uint64(i)) == nil {
					fr.sendRequestBlockToNode(TargetPublicHash, i)
				}
			}
		}
	}
}

func (fr *FormulatorNode) handleObserverMessage(ID string, m interface{}, RetryCount int) error {
	cp := fr.cs.cn.Provider()

	switch msg := m.(type) {
	case *BlockReqMessage:
		rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockReqMessage", msg.TargetHeight)

		fr.Lock()
		defer fr.Unlock()

		Height := cp.Height()
		if msg.TargetHeight <= fr.lastGenHeight && fr.lastGenTime+int64(30*time.Second) > time.Now().UnixNano() {
			return nil
		}
		if fr.lastReqMessage != nil {
			if msg.TargetHeight <= fr.lastReqMessage.TargetHeight {
				return nil
			}
		}
		if msg.TargetHeight <= Height {
			return nil
		}
		if msg.TargetHeight > Height+1 {
			if RetryCount >= 10 {
				return nil
			}

			if RetryCount == 0 {
				Count := uint32(msg.TargetHeight - Height - 1)
				if Count > 10 {
					Count = 10
				}
				for i := uint32(1); i <= Count; i++ {
					sm := &p2p.RequestMessage{
						Height: Height + i,
					}
					if err := fr.ms.SendTo(ID, sm); err != nil {
						return err
					}
				}
			}
			go func() {
				time.Sleep(50 * time.Millisecond)
				fr.handleObserverMessage(ID, m, RetryCount+1)
			}()
			return nil
		}

		Top, err := fr.cs.rt.TopRank(int(msg.TimeoutCount))
		if err != nil {
			return err
		}
		if msg.Formulator != Top.Address {
			return ErrInvalidRequest
		}
		if msg.Formulator != fr.Config.Formulator {
			return ErrInvalidRequest
		}
		if msg.FormulatorPublicHash != common.NewPublicHash(fr.key.PublicKey()) {
			return ErrInvalidRequest
		}
		if msg.PrevHash != cp.LastHash() {
			return ErrInvalidRequest
		}
		if msg.TargetHeight != Height+1 {
			return ErrInvalidRequest
		}
		fr.lastReqMessage = msg

		var wg sync.WaitGroup
		wg.Add(1)
		go func(req *BlockReqMessage) error {
			wg.Done()

			fr.Lock()
			defer fr.Unlock()

			fr.genLock.Lock()
			defer fr.genLock.Unlock()

			return fr.genBlock(ID, req)
		}(msg)
		wg.Wait()
		return nil
	case *BlockObSignMessage:
		rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockObSignMessage", msg.TargetHeight)

		fr.Lock()
		defer fr.Unlock()

		TargetHeight := fr.cs.cn.Provider().Height() + 1
		if msg.TargetHeight < TargetHeight {
			return nil
		}
		if msg.TargetHeight >= fr.lastReqMessage.TargetHeight+10 {
			return ErrInvalidRequest
		}
		fr.lastObSignMessageMap[msg.TargetHeight] = msg

		for len(fr.lastGenMessages) > 0 {
			GenMessage := fr.lastGenMessages[0]
			if GenMessage.Block.Header.Height < TargetHeight {
				if len(fr.lastGenMessages) > 1 {
					fr.lastGenMessages = fr.lastGenMessages[1:]
					fr.lastContextes = fr.lastContextes[1:]
				} else {
					fr.lastGenMessages = []*BlockGenMessage{}
					fr.lastContextes = []*types.Context{}
				}
				continue
			}
			if GenMessage.Block.Header.Height > TargetHeight {
				break
			}
			sm, has := fr.lastObSignMessageMap[GenMessage.Block.Header.Height]
			if !has {
				break
			}
			if GenMessage.Block.Header.Height == sm.TargetHeight {
				ctx := fr.lastContextes[0]

				if sm.BlockSign.HeaderHash != encoding.Hash(GenMessage.Block.Header) {
					return ErrInvalidRequest
				}

				b := &types.Block{
					Header:                GenMessage.Block.Header,
					TransactionTypes:      GenMessage.Block.TransactionTypes,
					Transactions:          GenMessage.Block.Transactions,
					TransactionSignatures: GenMessage.Block.TransactionSignatures,
					TransactionResults:    GenMessage.Block.TransactionResults,
					Signatures:            append([]common.Signature{GenMessage.GeneratorSignature}, sm.ObserverSignatures...),
				}
				if err := fr.cs.ct.ConnectBlockWithContext(b, ctx); err != nil {
					return err
				}
				fr.broadcastStatus()
				fr.cleanPool(b)
				rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockConnected", b.Header.Generator.String(), b.Header.Height, len(b.Transactions))

				fr.statusLock.Lock()
				if status, has := fr.obStatusMap[ID]; has {
					if status.Height < GenMessage.Block.Header.Height {
						status.Height = GenMessage.Block.Header.Height
					}
				}
				fr.statusLock.Unlock()

				if len(fr.lastGenMessages) > 1 {
					fr.lastGenMessages = fr.lastGenMessages[1:]
					fr.lastContextes = fr.lastContextes[1:]
				} else {
					fr.lastGenMessages = []*BlockGenMessage{}
					fr.lastContextes = []*types.Context{}
				}
			}
		}
		return nil
	case *p2p.BlockMessage:
		if err := fr.addBlock(msg.Block); err != nil {
			if err == chain.ErrFoundForkedBlock {
				panic(err)
			}
			return err
		}

		fr.statusLock.Lock()
		if status, has := fr.obStatusMap[ID]; has {
			lastHeight := msg.Block.Header.Height
			if status.Height < lastHeight {
				status.Height = lastHeight
			}
		}
		fr.statusLock.Unlock()

		fr.tryRequestNext()
		return nil
	case *p2p.StatusMessage:
		fr.statusLock.Lock()
		if status, has := fr.obStatusMap[ID]; has {
			if status.Height < msg.Height {
				status.Version = msg.Version
				status.Height = msg.Height
				status.LastHash = msg.LastHash
			}
		}
		fr.statusLock.Unlock()

		TargetHeight := cp.Height() + 1
		for TargetHeight <= msg.Height {
			if !fr.requestTimer.Exist(TargetHeight) {
				if fr.blockQ.Find(uint64(TargetHeight)) == nil {
					sm := &p2p.RequestMessage{
						Height: TargetHeight,
					}
					if err := fr.ms.SendTo(ID, sm); err != nil {
						return err
					}
					fr.requestTimer.Add(TargetHeight, 2*time.Second, ID)
				}
			}
			TargetHeight++
		}
		return nil
	case *p2p.TransactionMessage:
		if fr.txWaitQ.Size() > 200000 {
			return txpool.ErrTransactionPoolOverflowed
		}
		fr.txWaitQ.Push(&p2p.TxMsgItem{
			Message: msg,
			PeerID:  ID,
		})
		return nil
	default:
		panic(p2p.ErrUnknownMessage) //TEMP
		return p2p.ErrUnknownMessage
	}
}

func (fr *FormulatorNode) addBlock(b *types.Block) error {
	cp := fr.cs.cn.Provider()
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
		if item := fr.blockQ.FindOrInsert(b, uint64(b.Header.Height)); item != nil {
			old := item.(*types.Block)
			if encoding.Hash(old.Header) != encoding.Hash(b.Header) {
				//TODO : critical error signal
				return chain.ErrFoundForkedBlock
			}
		}
	}
	return nil
}

func (fr *FormulatorNode) tryRequestNext() {
	fr.requestLock.Lock()
	defer fr.requestLock.Unlock()

	TargetHeight := fr.cs.cn.Provider().Height() + 1
	if !fr.requestTimer.Exist(TargetHeight) {
		if fr.blockQ.Find(uint64(TargetHeight)) == nil {
			fr.statusLock.Lock()
			var TargetPubHash string
			for pubhash, status := range fr.obStatusMap {
				if TargetHeight <= status.Height {
					TargetPubHash = pubhash
					break
				}
			}
			fr.statusLock.Unlock()

			if len(TargetPubHash) > 0 {
				fr.sendRequestBlockTo(TargetPubHash, TargetHeight)
			}
		}
	}
}

func (fr *FormulatorNode) cleanPool(b *types.Block) {
	for i, tx := range b.Transactions {
		t := b.TransactionTypes[i]
		TxHash := chain.HashTransactionByType(fr.cs.cn.Provider().ChainID(), t, tx)
		fr.txpool.Remove(TxHash, tx)
		fr.txQ.Remove(string(TxHash[:]))
	}
}

func (fr *FormulatorNode) genBlock(ID string, msg *BlockReqMessage) error {
	cp := fr.cs.cn.Provider()

	fr.lastGenMessages = []*BlockGenMessage{}
	fr.lastObSignMessageMap = map[uint32]*BlockObSignMessage{}
	fr.lastContextes = []*types.Context{}

	start := time.Now().UnixNano()
	StartTime := uint64(time.Now().UnixNano())
	StartBlockTime := StartTime
	bNoDelay := false

	RemainBlocks := fr.cs.maxBlocksPerFormulator
	if msg.TimeoutCount == 0 {
		RemainBlocks = fr.cs.maxBlocksPerFormulator - fr.cs.blocksBySameFormulator
	}

	LastTimestamp := cp.LastTimestamp()
	if StartBlockTime < LastTimestamp {
		StartBlockTime = LastTimestamp + uint64(time.Millisecond)
	} else if StartBlockTime > LastTimestamp+uint64(RemainBlocks)*uint64(500*time.Millisecond) {
		bNoDelay = true
	}

	tx := &vault.Transfer{
		Amount: amount.NewCoinAmount(1, 0),
		To:     common.MustParseAddress("3CUsUpv9v"),
	}
	fc := encoding.Factory("transaction")
	t, err := fc.TypeOf(tx)
	if err != nil {
		return err
	}
	k, _ := key.NewMemoryKeyFromString("04aeb041bef9f8802080c2d7f06a1cf440d6c0e4c5050fd2bf3fa73942a9128b")
	TxHash := chain.HashTransactionByType(fr.cs.cn.Provider().ChainID(), t, tx)
	sig, _ := k.Sign(TxHash)
	signer := common.MustParsePublicHash("38dWpxjJY1RwqyzCfhuaTT9YjyyuxJktaWhRBq8XUZ5")

	var lastHeader *types.Header
	ctx := fr.cs.ct.NewContext()
	for i := uint32(0); i < RemainBlocks; i++ {
		var TimeoutCount uint32
		if i == 0 {
			TimeoutCount = msg.TimeoutCount
		} else {
			ctx = ctx.NextContext(encoding.Hash(lastHeader), lastHeader.Timestamp)
		}

		Timestamp := StartBlockTime
		log.Println("StartBlockTime", StartBlockTime, bNoDelay, Timestamp > StartTime+uint64(3*time.Second), StartTime, Timestamp, StartTime+uint64(3*time.Second))
		if bNoDelay || Timestamp > StartTime+uint64(3*time.Second) {
			Timestamp += uint64(i) * uint64(time.Millisecond)
		} else {
			Timestamp += uint64(i) * uint64(500*time.Millisecond)
		}
		if Timestamp <= ctx.LastTimestamp() {
			Timestamp = ctx.LastTimestamp() + 1
		}

		var buffer bytes.Buffer
		enc := encoding.NewEncoder(&buffer)
		if err := enc.EncodeUint32(TimeoutCount); err != nil {
			return err
		}
		bc := chain.NewBlockCreator(fr.cs.cn, ctx, msg.Formulator, buffer.Bytes())
		if err := bc.Init(); err != nil {
			return err
		}

		/*
				timer := time.NewTimer(200 * time.Millisecond)

				rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockGenBegin", msg.TargetHeight)

				fr.txpool.Lock() // Prevent delaying from TxPool.Push
				Count := 0
			TxLoop:
				for {
					select {
					case <-timer.C:
						break TxLoop
					default:
						sn := ctx.Snapshot()
						item := fr.txpool.UnsafePop(ctx)
						ctx.Revert(sn)
						if item == nil {
							break TxLoop
						}
						if err := bc.UnsafeAddTx(fr.Config.Formulator, item.TxType, item.TxHash, item.Transaction, item.Signatures, item.Signers); err != nil {
							rlog.Println(err)
							continue
						}
						Count++
						if Count > fr.Config.MaxTransactionsPerBlock {
							break TxLoop
						}
					}
				}
				fr.txpool.Unlock() // Prevent delaying from TxPool.Push
		*/
		//for q := 0; q < fr.Config.MaxTransactionsPerBlock; q++ {
		for q := 0; q < 5000; q++ {
			if err := bc.UnsafeAddTx(fr.Config.Formulator, t, TxHash, tx, []common.Signature{sig}, []common.PublicHash{signer}); err != nil {
				rlog.Println(err)
				continue
			}
		}

		b, err := bc.Finalize(Timestamp)
		if err != nil {
			rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockGenMessage.Finalize", err)
			return err
		}

		nm := &BlockGenMessage{
			Block: b,
		}
		lastHeader = &b.Header

		if sig, err := fr.key.Sign(encoding.Hash(b.Header)); err != nil {
			rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockGenMessage.Sign", nm.Block.Header.Height, err)
			return err
		} else {
			nm.GeneratorSignature = sig
		}

		if err := fr.ms.SendTo(ID, nm); err != nil {
			rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockGenMessage.SendTo", nm.Block.Header.Height, err)
			return err
		}
		rlog.Println("Formulator", fr.Config.Formulator.String(), "BlockGenMessage", nm.Block.Header.Height, len(nm.Block.Transactions))

		fr.lastGenMessages = append(fr.lastGenMessages, nm)
		fr.lastContextes = append(fr.lastContextes, ctx)
		fr.lastGenHeight = ctx.TargetHeight()
		fr.lastGenTime = time.Now().UnixNano()

		ExpectedTime := time.Duration(i+1) * 500 * time.Millisecond
		if i >= uint32(fr.Config.MaxTransactionsPerBlock)-3 {
			ExpectedTime = 500*time.Duration(fr.Config.MaxTransactionsPerBlock-3)*time.Millisecond + time.Duration(i-7+1)*100*time.Millisecond
		}
		PastTime := time.Duration(time.Now().UnixNano() - start)
		if !bNoDelay && ExpectedTime > PastTime {
			fr.Unlock()
			time.Sleep(ExpectedTime - PastTime)
			fr.Lock()
		}
	}
	return nil
}

func (fr *FormulatorNode) sendMessagePacket(Priority int, Target common.PublicHash, bs []byte, Height uint32) {
	fr.sendQueues[Priority].Push(&p2p.SendMessageItem{
		Target: Target,
		Packet: bs,
		Height: Height,
	})
}

func (fr *FormulatorNode) sendMessage(Priority int, Target common.PublicHash, m interface{}) {
	fr.sendQueues[Priority].Push(&p2p.SendMessageItem{
		Target:  Target,
		Message: m,
	})
}

func (fr *FormulatorNode) broadcastMessage(Priority int, m interface{}) {
	fr.sendQueues[Priority].Push(&p2p.SendMessageItem{
		Message: m,
	})
}

func (fr *FormulatorNode) limitCastMessage(Priority int, m interface{}) {
	defer debug.Start("limitCastMessage").Stop()
	fr.sendQueues[Priority].Push(&p2p.SendMessageItem{
		Message: m,
		Limit:   3,
	})
}

func (fr *FormulatorNode) exceptLimitCastMessage(Priority int, Target common.PublicHash, m interface{}) {
	defer debug.Start("exceptLimitCastMessage").Stop()
	fr.sendQueues[Priority].Push(&p2p.SendMessageItem{
		Target:  Target,
		Message: m,
		Limit:   3,
	})
}
