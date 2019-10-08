package p2p

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fletaio/fleta_testnet/common/queue"
	"github.com/fletaio/fleta_testnet/common/util"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/encoding"
)

// TCPPeer manages send and recv of the connection
type TCPPeer struct {
	sync.Mutex
	conn           net.Conn
	id             string
	name           string
	guessHeight    uint32
	writeQueue     *queue.Queue
	writeHighQueue *queue.Queue
	isClose        bool
	connectedTime  int64
	pingCount      uint64
	pingType       uint16
}

// NewTCPPeer returns a TCPPeer
func NewTCPPeer(conn net.Conn, ID string, Name string, connectedTime int64) *TCPPeer {
	if len(Name) == 0 {
		Name = ID
	}
	p := &TCPPeer{
		conn:           conn,
		id:             ID,
		name:           Name,
		writeQueue:     queue.NewQueue(),
		writeHighQueue: queue.NewQueue(),
		connectedTime:  connectedTime,
		pingType:       types.DefineHashedType("p2p.PingMessage"),
	}

	go func() {
		defer p.Close()

		pingCountLimit := uint64(3)
		pingTicker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-pingTicker.C:
				if err := p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return
				}
				_, err := p.conn.Write(util.Uint16ToBytes(p.pingType))
				if err != nil {
					return
				}
				if atomic.AddUint64(&p.pingCount, 1) > pingCountLimit {
					return
				}
			default:
				if p.isClose {
					return
				}
				var bs []byte
				if p.writeHighQueue.Size() > 0 {
					v := p.writeHighQueue.Pop()
					bs = v.([]byte)
				} else if p.writeQueue.Size() > 0 {
					v := p.writeQueue.Pop()
					bs = v.([]byte)
				} else {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				var buffer bytes.Buffer
				buffer.Write(bs[:2])
				buffer.Write(make([]byte, 4))
				/*
					if len(bs) > 2 {
						zw := gzip.NewWriter(&buffer)
						zw.Write(bs[2:])
						zw.Flush()
						zw.Close()
					}
				*/
				buffer.Write(bs[2:])
				wbs := buffer.Bytes()
				binary.LittleEndian.PutUint32(wbs[2:], uint32(len(wbs)-6))
				if err := p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return
				}
				_, err := p.conn.Write(wbs)
				if err != nil {
					return
				}
			}
		}
	}()
	return p
}

// ID returns the id of the peer
func (p *TCPPeer) ID() string {
	return p.id
}

// Name returns the name of the peer
func (p *TCPPeer) Name() string {
	return p.name
}

// Close closes TCPPeer
func (p *TCPPeer) Close() {
	p.isClose = true
	p.conn.Close()
}

// ReadMessageData returns a message data
func (p *TCPPeer) ReadMessageData() (interface{}, []byte, error) {
	var t uint16
	for {
		if v, _, err := ReadUint16(p.conn); err != nil {
			return nil, nil, err
		} else {
			atomic.StoreUint64(&p.pingCount, 0)
			if v == p.pingType {
				continue
			} else {
				t = v
				break
			}
		}
	}

	if Len, _, err := ReadUint32(p.conn); err != nil {
		return nil, nil, err
	} else if Len == 0 {
		return nil, nil, ErrUnknownMessage
	} else {
		zbs := make([]byte, Len)
		if _, err := FillBytes(p.conn, zbs); err != nil {
			return nil, nil, err
		}
		/*
			zr, err := gzip.NewReader(bytes.NewReader(zbs))
			if err != nil {
				return nil, nil, err
			}
			defer zr.Close()
		*/

		fc := encoding.Factory("message")
		m, err := fc.Create(t)
		if err != nil {
			return nil, nil, err
		}
		/*
			bs, err := ioutil.ReadAll(zr)
			if err != nil {
				return nil, nil, err
			}
		*/
		bs := zbs
		if err := encoding.Unmarshal(bs, &m); err != nil {
			return nil, nil, err
		}
		return m, bs, nil
	}
}

// Send sends a message to the TCPPeer
func (p *TCPPeer) Send(m interface{}) error {
	data, err := MessageToBytes(m)
	if err != nil {
		return err
	}
	p.SendRaw(data)
	return nil
}

// SendRaw sends bytes to the TCPPeer
func (p *TCPPeer) SendRaw(bs []byte) {
	p.writeQueue.Push(bs)
}

func (p *TCPPeer) SendHigh(m interface{}) error {
	data, err := MessageToBytes(m)
	if err != nil {
		return err
	}
	p.SendHighRaw(data)
	return nil
}

func (p *TCPPeer) SendHighRaw(bs []byte) {
	p.writeHighQueue.Push(bs)
}

// UpdateGuessHeight updates the guess height of the TCPPeer
func (p *TCPPeer) UpdateGuessHeight(height uint32) {
	p.Lock()
	defer p.Unlock()

	p.guessHeight = height
}

// GuessHeight updates the guess height of the TCPPeer
func (p *TCPPeer) GuessHeight() uint32 {
	return p.guessHeight
}

// ConnectedTime returns peer connected time
func (p *TCPPeer) ConnectedTime() int64 {
	return p.connectedTime
}
