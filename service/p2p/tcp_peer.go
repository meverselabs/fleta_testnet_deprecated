package p2p

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fletaio/fleta_testnet/common/util"
	"github.com/fletaio/fleta_testnet/core/types"
	"github.com/fletaio/fleta_testnet/encoding"
)

// TCPPeer manages send and recv of the connection
type TCPPeer struct {
	sync.Mutex
	conn          net.Conn
	id            string
	name          string
	guessHeight   uint32
	isClose       bool
	connectedTime int64
	pingCount     uint64
	pingType      uint16
}

// NewTCPPeer returns a TCPPeer
func NewTCPPeer(conn net.Conn, ID string, Name string, connectedTime int64) *TCPPeer {
	if len(Name) == 0 {
		Name = ID
	}
	p := &TCPPeer{
		conn:          conn,
		id:            ID,
		name:          Name,
		connectedTime: connectedTime,
		pingType:      types.DefineHashedType("p2p.PingMessage"),
	}

	go func() {
		defer p.Close()

		pingCountLimit := uint64(3)
		pingTicker := time.NewTicker(10 * time.Second)
		for {
			if p.isClose {
				return
			}
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
	r := NewCopyReader(p.conn)

	var t uint16
	for {
		if v, _, err := ReadUint16(r); err != nil {
			return nil, nil, err
		} else {
			atomic.StoreUint64(&p.pingCount, 0)
			if v == p.pingType {
				r.Reset()
				continue
			} else {
				t = v
				break
			}
		}
	}

	if cp, _, err := ReadUint8(r); err != nil {
		return nil, nil, err
	} else if Len, _, err := ReadUint32(r); err != nil {
		return nil, nil, err
	} else if Len == 0 {
		return nil, nil, ErrUnknownMessage
	} else {
		zbs := make([]byte, Len)
		if _, err := FillBytes(r, zbs); err != nil {
			return nil, nil, err
		}
		var bs []byte
		if cp == 1 {
			zr, err := gzip.NewReader(bytes.NewReader(zbs))
			if err != nil {
				return nil, nil, err
			}
			defer zr.Close()

			v, err := ioutil.ReadAll(zr)
			if err != nil {
				return nil, nil, err
			}
			bs = v
		} else {
			bs = zbs
		}

		fc := encoding.Factory("message")
		m, err := fc.Create(t)
		if err != nil {
			return nil, nil, err
		}
		if err := encoding.Unmarshal(bs, &m); err != nil {
			return nil, nil, err
		}
		return m, r.Bytes(), nil
	}
}

// Send sends a message to the TCPPeer
func (p *TCPPeer) Send(m interface{}) error {
	data, err := MessageToPacket(m)
	if err != nil {
		return err
	}
	if err := p.SendRaw(data); err != nil {
		return err
	}
	return nil
}

// SendRaw sends packet to the TCPPeer
func (p *TCPPeer) SendRaw(bs []byte) error {
	if err := p.conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err := p.conn.Write(bs); err != nil {
		return err
	}
	return nil
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
