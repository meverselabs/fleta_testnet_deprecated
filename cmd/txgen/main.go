package main

import (
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/fletaio/fleta_testnet/cmd/app"
	"github.com/fletaio/fleta_testnet/cmd/closer"
	"github.com/fletaio/fleta_testnet/cmd/config"
	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/amount"
	"github.com/fletaio/fleta_testnet/common/key"
	"github.com/fletaio/fleta_testnet/common/rlog"
	"github.com/fletaio/fleta_testnet/core/backend"
	_ "github.com/fletaio/fleta_testnet/core/backend/badger_driver"
	_ "github.com/fletaio/fleta_testnet/core/backend/buntdb_driver"
	"github.com/fletaio/fleta_testnet/core/chain"
	"github.com/fletaio/fleta_testnet/core/pile"
	"github.com/fletaio/fleta_testnet/core/txpool"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/pof"
	"github.com/fletaio/fleta_testnet/process/admin"
	"github.com/fletaio/fleta_testnet/process/formulator"
	"github.com/fletaio/fleta_testnet/process/gateway"
	"github.com/fletaio/fleta_testnet/process/payment"
	"github.com/fletaio/fleta_testnet/process/vault"
	"github.com/fletaio/fleta_testnet/service/apiserver"
	"github.com/fletaio/fleta_testnet/service/p2p"
)

// Config is a configuration for the cmd
type Config struct {
	SeedNodeMap    map[string]string
	NodeKeyHex     string
	ObserverKeys   []string
	Port           int
	APIPort        int
	StoreRoot      string
	BackendVersion int
	RLogHost       string
	RLogPath       string
	UseRLog        bool
}

func main() {
	var cfg Config
	if err := config.LoadFile("./config.toml", &cfg); err != nil {
		panic(err)
	}
	if len(cfg.StoreRoot) == 0 {
		cfg.StoreRoot = "./ndata"
	}
	if len(cfg.RLogHost) > 0 && cfg.UseRLog {
		if len(cfg.RLogPath) == 0 {
			cfg.RLogPath = "./ndata_rlog"
		}
		rlog.SetRLogHost(cfg.RLogHost)
		rlog.Enablelogger(cfg.RLogPath)
	}

	var ndkey key.Key
	if len(cfg.NodeKeyHex) > 0 {
		if bs, err := hex.DecodeString(cfg.NodeKeyHex); err != nil {
			panic(err)
		} else if Key, err := key.NewMemoryKeyFromBytes(bs); err != nil {
			panic(err)
		} else {
			ndkey = Key
		}
	} else {
		if bs, err := ioutil.ReadFile("./ndkey.key"); err != nil {
			k, err := key.NewMemoryKey()
			if err != nil {
				panic(err)
			}

			fs, err := os.Create("./ndkey.key")
			if err != nil {
				panic(err)
			}
			fs.Write(k.Bytes())
			fs.Close()
			ndkey = k
		} else {
			if Key, err := key.NewMemoryKeyFromBytes(bs); err != nil {
				panic(err)
			} else {
				ndkey = Key
			}
		}
	}

	ObserverKeys := []common.PublicHash{}
	for _, k := range cfg.ObserverKeys {
		pubhash, err := common.ParsePublicHash(k)
		if err != nil {
			panic(err)
		}
		ObserverKeys = append(ObserverKeys, pubhash)
	}
	SeedNodeMap := map[common.PublicHash]string{}
	for k, netAddr := range cfg.SeedNodeMap {
		pubhash, err := common.ParsePublicHash(k)
		if err != nil {
			panic(err)
		}
		SeedNodeMap[pubhash] = netAddr
	}

	MaxBlocksPerFormulator := uint32(10)
	ChainID := uint8(0x01)
	Name := "FLETA Testnet"
	Version := uint16(0x0001)

	cm := closer.NewManager()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigc
		cm.CloseAll()
	}()
	defer cm.CloseAll()

	var back backend.StoreBackend
	var cdb *pile.DB
	switch cfg.BackendVersion {
	case 0:
		contextDB, err := backend.Create("badger", cfg.StoreRoot)
		if err != nil {
			panic(err)
		}
		back = contextDB
	case 1:
		contextDB, err := backend.Create("buntdb", cfg.StoreRoot+"/context")
		if err != nil {
			panic(err)
		}
		chainDB, err := pile.Open(cfg.StoreRoot + "/chain")
		if err != nil {
			panic(err)
		}
		chainDB.SetSyncMode(true)
		back = contextDB
		cdb = chainDB
	}
	st, err := chain.NewStore(back, cdb, ChainID, Name, Version)
	if err != nil {
		panic(err)
	}
	cm.Add("store", st)

	if st.Height() > 0 {
		if _, err := cdb.GetData(st.Height(), 0); err != nil {
			panic(err)
		}
	}

	cs := pof.NewConsensus(MaxBlocksPerFormulator, ObserverKeys)
	app := app.NewFletaApp()
	cn := chain.NewChain(cs, app, st)
	cn.MustAddProcess(admin.NewAdmin(1))
	cn.MustAddProcess(vault.NewVault(2))
	cn.MustAddProcess(formulator.NewFormulator(3))
	cn.MustAddProcess(gateway.NewGateway(4))
	cn.MustAddProcess(payment.NewPayment(5))
	as := apiserver.NewAPIServer()
	cn.MustAddService(as)
	ws := NewWatcher()
	cn.MustAddService(ws)
	if err := cn.Init(); err != nil {
		panic(err)
	}
	cm.RemoveAll()
	cm.Add("chain", cn)

	if err := st.IterBlockAfterContext(func(b *types.Block) error {
		if cm.IsClosed() {
			return chain.ErrStoreClosed
		}
		if err := cn.ConnectBlock(b); err != nil {
			return err
		}
		return nil
	}); err != nil {
		if err == chain.ErrStoreClosed {
			return
		}
		panic(err)
	}

	nd := p2p.NewNode(ndkey, SeedNodeMap, cn, cfg.StoreRoot+"/peer")
	if err := nd.Init(); err != nil {
		panic(err)
	}
	cm.RemoveAll()
	cm.Add("node", nd)

	waitMap := map[common.Address]*chan struct{}{}
	if false {
		go func() {
			switch cfg.NodeKeyHex {
			case "74a4bb065b9553e18c5f6aab54bcb07db58f2950b09d3be024e20318512d97bb":
				Addrs = Addrs[:7500]
				//Addrs = Addrs[:1]
			case "f7b6a6291165b7d4cea6d16b911b6f2ba024aac6f160b230b9a04de876f3b045":
				Addrs = Addrs[7500:15000]
				//Addrs = Addrs[50:51]
			case "4bc61ab268197c465d471d67618a3bf385651a93ed5011b8db08f5dfdca43c1d":
				Addrs = Addrs[15000:22500]
				//Addrs = Addrs[100:101]
			case "bfe9f217f31f52a8e3e975c415d297ff201a4c4abfbcb921eb9013b0c21397f4":
				Addrs = Addrs[22500:30000]
				//Addrs = Addrs[150:151]
			default:
				Addrs = []common.Address{}
				//Addrs = Addrs[0:1]
			}

			for _, Addr := range Addrs {
				waitMap[Addr] = ws.addAddress(Addr)
			}

			for _, v := range Addrs {
				go func(Addr common.Address) {
					for {
						time.Sleep(5 * time.Second)

						Seq := st.Seq(Addr)
						key, _ := key.NewMemoryKeyFromString("fd1167aad31c104c9fceb5b8a4ffd3e20a272af82176352d3b6ac236d02bafd4")
						log.Println(Addr.String(), "Start Transaction", Seq)

						for i := 0; i < 2; i++ {
							Seq++
							tx := &vault.Transfer{
								Timestamp_: uint64(time.Now().UnixNano()),
								Seq_:       Seq,
								From_:      Addr,
								To:         Addr,
								Amount:     amount.NewCoinAmount(1, 0),
							}
							sig, err := key.Sign(chain.HashTransaction(ChainID, tx))
							if err != nil {
								panic(err)
							}
							if err := nd.AddTx(tx, []common.Signature{sig}); err != nil {
								panic(err)
							}
							time.Sleep(100 * time.Millisecond)
						}

						pCh := waitMap[Addr]

						if pCh == nil {
							log.Println(Addr)
						}

						for range *pCh {
							Seq++
							//log.Println(Addr.String(), "Execute Transaction", Seq)
							tx := &vault.Transfer{
								Timestamp_: uint64(time.Now().UnixNano()),
								Seq_:       Seq,
								From_:      Addr,
								To:         Addr,
								Amount:     amount.NewCoinAmount(1, 0),
							}
							sig, err := key.Sign(chain.HashTransaction(ChainID, tx))
							if err != nil {
								panic(err)
							}
							if err := nd.AddTx(tx, []common.Signature{sig}); err != nil {
								switch err {
								case txpool.ErrExistTransaction:
								case txpool.ErrTooFarSeq:
									Seq--
								}
								time.Sleep(100 * time.Millisecond)
								continue
							}
							time.Sleep(10 * time.Millisecond)
						}
					}
				}(v)
			}
		}()
	}

	go func() {
		for {
			b := <-ws.blockCh
			for i, t := range b.Transactions {
				res := b.TransactionResults[i]
				if res == 1 {
					if tx, is := t.(chain.AccountTransaction); is {

						CreatedAddr := common.NewAddress(b.Header.Height, uint16(i), 0)
						switch tx.(type) {
						case (*vault.IssueAccount):
							log.Println("Created", CreatedAddr.String())
						//case (*vault.Transfer):
						//	log.Println("Transfered", tx.(*vault.Transfer).To)
						default:
							pCh, has := waitMap[tx.From()]
							if has {
								(*pCh) <- struct{}{}
							}
						}
					}
				}
			}
		}
	}()

	go nd.Run(":" + strconv.Itoa(cfg.Port))
	go as.Run(":" + strconv.Itoa(cfg.APIPort))

	cm.Wait()
}

// Watcher provides json rpc and web service for the chain
type Watcher struct {
	sync.Mutex
	types.ServiceBase
	waitMap map[common.Address]*chan struct{}
	blockCh chan *types.Block
}

// NewWatcher returns a Watcher
func NewWatcher() *Watcher {
	s := &Watcher{
		waitMap: map[common.Address]*chan struct{}{},
		blockCh: make(chan *types.Block, 1000),
	}
	return s
}

// Name returns the name of the service
func (s *Watcher) Name() string {
	return "fleta.watcher"
}

// Init called when initialize service
func (s *Watcher) Init(pm types.ProcessManager, cn types.Provider) error {
	return nil
}

// OnLoadChain called when the chain loaded
func (s *Watcher) OnLoadChain(loader types.Loader) error {
	return nil
}

func (s *Watcher) addAddress(addr common.Address) *chan struct{} {
	ch := make(chan struct{})
	s.waitMap[addr] = &ch
	return &ch
}

// OnBlockConnected called when a block is connected to the chain
func (s *Watcher) OnBlockConnected(b *types.Block, events []types.Event, loader types.Loader) {
	s.blockCh <- b
}
