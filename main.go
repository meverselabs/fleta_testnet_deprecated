package main // import "github.com/fletaio/fleta_testnet"

import (
	"encoding/hex"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/fletaio/fleta_testnet/cmd/app"
	"github.com/fletaio/fleta_testnet/common"
	"github.com/fletaio/fleta_testnet/common/key"
	"github.com/fletaio/fleta_testnet/core/backend"
	_ "github.com/fletaio/fleta_testnet/core/backend/badger_driver"
	_ "github.com/fletaio/fleta_testnet/core/backend/buntdb_driver"
	"github.com/fletaio/fleta_testnet/core/chain"
	"github.com/fletaio/fleta_testnet/core/pile"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/pof"
	"github.com/fletaio/fleta_testnet/process/admin"
	"github.com/fletaio/fleta_testnet/process/formulator"
	"github.com/fletaio/fleta_testnet/process/gateway"
	"github.com/fletaio/fleta_testnet/process/payment"
	"github.com/fletaio/fleta_testnet/process/vault"
)

func main() {
	if err := test(); err != nil {
		panic(err)
	}
}

func test() error {
	os.RemoveAll("./_test")
	defer os.RemoveAll("./_test")

	obstrs := []string{
		"cca49818f6c49cf57b6c420cdcd98fcae08850f56d2ff5b8d287fddc7f9ede08",
		"39f1a02bed5eff3f6247bb25564cdaef20d410d77ef7fc2c0181b1d5b31ce877",
		"2b97bc8f21215b7ed085cbbaa2ea020ded95463deef6cbf31bb1eadf826d4694",
		"3b43d728deaa62d7c8790636bdabbe7148a6641e291fd1f94b157673c0172425",
		"e6cf2724019000a3f703db92829ecbd646501c0fd6a5e97ad6774d4ad621f949",
	}
	obkeys := make([]key.Key, 0, len(obstrs))
	NetAddressMap := map[common.PublicHash]string{}
	FrNetAddressMap := map[common.PublicHash]string{}
	HwNetAddressMap := map[common.PublicHash]string{}
	ObserverKeys := make([]common.PublicHash, 0, len(obstrs))
	for i, v := range obstrs {
		if bs, err := hex.DecodeString(v); err != nil {
			panic(err)
		} else if Key, err := key.NewMemoryKeyFromBytes(bs); err != nil {
			panic(err)
		} else {
			obkeys = append(obkeys, Key)
			pubhash := common.NewPublicHash(Key.PublicKey())
			ObserverKeys = append(ObserverKeys, pubhash)
			NetAddressMap[pubhash] = ":400" + strconv.Itoa(i)
			FrNetAddressMap[pubhash] = "ws://localhost:500" + strconv.Itoa(i)
			HwNetAddressMap[pubhash] = "ws://localhost:490" + strconv.Itoa(i)
		}
	}

	MaxBlocksPerFormulator := uint32(10)
	ChainID := uint8(0x01)

	for i, obkey := range obkeys {
		back, err := backend.Create("buntdb", "./_test/odata_"+strconv.Itoa(i)+"/context")
		if err != nil {
			return err
		}
		cdb, err := pile.Open("./_test/odata_" + strconv.Itoa(i) + "/chain")
		if err != nil {
			return err
		}
		cdb.SetSyncMode(true)
		st, err := chain.NewStore(back, cdb, ChainID, "FLEAT Mainnet", 0x0001)
		if err != nil {
			return err
		}
		defer st.Close()

		cs := pof.NewConsensus(MaxBlocksPerFormulator, ObserverKeys)
		app := app.NewFletaApp()
		cn := chain.NewChain(cs, app, st)
		cn.MustAddProcess(admin.NewAdmin(1))
		vp := vault.NewVault(2)
		cn.MustAddProcess(vp)
		fp := formulator.NewFormulator(3)
		cn.MustAddProcess(fp)
		cn.MustAddProcess(gateway.NewGateway(4))
		cn.MustAddProcess(payment.NewPayment(5))
		if err := cn.Init(); err != nil {
			return err
		}

		ob := pof.NewObserverNode(obkey, NetAddressMap, cs)
		if err := ob.Init(); err != nil {
			panic(err)
		}

		go ob.Run(":400"+strconv.Itoa(i), ":500"+strconv.Itoa(i))
	}

	frstrs := []string{
		"f732e0551cc030f7946c70d03036214845a7eeb6b3d39266ddb04429c304fb85",
	}
	frkeys := make([]key.Key, 0, len(frstrs))
	for _, v := range frstrs {
		if bs, err := hex.DecodeString(v); err != nil {
			panic(err)
		} else if Key, err := key.NewMemoryKeyFromBytes(bs); err != nil {
			panic(err)
		} else {
			frkeys = append(frkeys, Key)
		}
	}

	ndstrs := []string{
		"a5de9897e9c405ab75eb2265fb424bfa25b90996ff4cec597b434023508a10a5",
	}
	NdNetAddressMap := map[common.PublicHash]string{}
	ndkeys := make([]key.Key, 0, len(ndstrs))
	for i, v := range ndstrs {
		if bs, err := hex.DecodeString(v); err != nil {
			panic(err)
		} else if Key, err := key.NewMemoryKeyFromBytes(bs); err != nil {
			panic(err)
		} else {
			ndkeys = append(ndkeys, Key)
			pubhash := common.NewPublicHash(Key.PublicKey())
			NdNetAddressMap[pubhash] = ":601" + strconv.Itoa(i)
		}
	}

	for i, frkey := range frkeys {
		back, err := backend.Create("buntdb", "./_test/fdata_"+strconv.Itoa(i)+"/context")
		if err != nil {
			return err
		}
		cdb, err := pile.Open("./_test/fdata_" + strconv.Itoa(i) + "/chain")
		if err != nil {
			return err
		}
		cdb.SetSyncMode(true)
		st, err := chain.NewStore(back, cdb, ChainID, "FLEAT Mainnet", 0x0001)
		if err != nil {
			return err
		}
		defer st.Close()

		cs := pof.NewConsensus(MaxBlocksPerFormulator, ObserverKeys)
		app := app.NewFletaApp()
		cn := chain.NewChain(cs, app, st)
		cn.MustAddProcess(admin.NewAdmin(1))
		vp := vault.NewVault(2)
		cn.MustAddProcess(vp)
		fp := formulator.NewFormulator(3)
		cn.MustAddProcess(fp)
		cn.MustAddProcess(gateway.NewGateway(4))
		cn.MustAddProcess(payment.NewPayment(5))
		if err := cn.Init(); err != nil {
			return err
		}

		fr := pof.NewFormulatorNode(&pof.FormulatorConfig{
			Formulator:              common.MustParseAddress("385ujsGNZt"),
			MaxTransactionsPerBlock: 10000,
		}, frkey, frkey, FrNetAddressMap, NdNetAddressMap, cs, "./_test/fdata_"+strconv.Itoa(i)+"/peer")
		if err := fr.Init(); err != nil {
			panic(err)
		}

		go fr.Run(":600" + strconv.Itoa(i))
	}

	/*
		for i, ndkey := range ndkeys {
			back, err := backend.Create("buntdb", "./_test/ndata_"+strconv.Itoa(i)+"/context")
			if err != nil {
				return err
			}
			cdb, err := pile.Open("./_test/ndata_" + strconv.Itoa(i) + "/chain")
			if err != nil {
				return err
			}
			cdb.SetSyncMode(true)
			st, err := chain.NewStore(back, cdb, ChainID, "FLEAT Mainnet", 0x0001)
			if err != nil {
				return err
			}
			defer st.Close()

			cs := pof.NewConsensus(MaxBlocksPerFormulator, ObserverKeys)
			app := app.NewFletaApp()
			cn := chain.NewChain(cs, app, st)
			cn.MustAddProcess(admin.NewAdmin(1))
			vp := vault.NewVault(2)
			cn.MustAddProcess(vp)
			fp := formulator.NewFormulator(3)
			cn.MustAddProcess(fp)
			cn.MustAddProcess(gateway.NewGateway(4))
			cn.MustAddProcess(payment.NewPayment(5))
			ws := NewWatcher()
			cn.MustAddService(ws)
			if err := cn.Init(); err != nil {
				return err
			}

			nd := p2p.NewNode(ndkey, NdNetAddressMap, cn, "./_test/ndata_"+strconv.Itoa(i)+"/peer")
			if err := nd.Init(); err != nil {
				panic(err)
			}

			go func() {
				//time.Sleep(60 * time.Second)

				if true {
					go func() {
						waitMap := map[common.Address]*chan struct{}{}
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

									for i := 0; i < 1; i++ {
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

				nd.Run(":601" + strconv.Itoa(i))
			}()
		}
	*/

	select {}
	return nil
}

// Watcher provides json rpc and web service for the chain
type Watcher struct {
	sync.Mutex
	types.ServiceBase
	waitMap map[common.Address]*chan struct{}
}

// NewWatcher returns a Watcher
func NewWatcher() *Watcher {
	s := &Watcher{
		waitMap: map[common.Address]*chan struct{}{},
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
					pCh, has := s.waitMap[tx.From()]
					if has {
						(*pCh) <- struct{}{}
					}
				}
			}
		}
	}
}
