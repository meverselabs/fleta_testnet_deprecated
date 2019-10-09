package p2p

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fletaio/fleta_testnet/common/queue"
	"github.com/fletaio/fleta_testnet/common/util"
	"github.com/fletaio/fleta_testnet/encoding"
)

// WebsocketPeer manages send and recv of the connection
type WebsocketPeer struct {
	sync.Mutex
	conn          *websocket.Conn
	id            string
	name          string
	guessHeight   uint32
	writeQueue    *queue.Queue
	isClose       bool
	connectedTime int64
	pingCount     uint64
}

// NewWebsocketPeer returns a WebsocketPeer
func NewWebsocketPeer(conn *websocket.Conn, ID string, Name string, connectedTime int64) *WebsocketPeer {
	if len(Name) == 0 {
		Name = ID
	}
	p := &WebsocketPeer{
		conn:          conn,
		id:            ID,
		name:          Name,
		writeQueue:    queue.NewQueue(),
		connectedTime: connectedTime,
	}
	conn.EnableWriteCompression(false)
	conn.SetPingHandler(func(appData string) error {
		atomic.StoreUint64(&p.pingCount, 0)
		return nil
	})

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
				if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
				if atomic.AddUint64(&p.pingCount, 1) > pingCountLimit {
					return
				}
			default:
				v := p.writeQueue.Pop()
				if v == nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				wbs := v.([]byte)
				if err := p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return
				}
				if err := p.conn.WriteMessage(websocket.BinaryMessage, wbs); err != nil {
					return
				}
			}
		}
	}()
	return p
}

// ID returns the id of the peer
func (p *WebsocketPeer) ID() string {
	return p.id
}

// Name returns the name of the peer
func (p *WebsocketPeer) Name() string {
	return p.name
}

// Close closes WebsocketPeer
func (p *WebsocketPeer) Close() {
	p.isClose = true
	p.conn.Close()
}

// ReadMessageData returns a message data
func (p *WebsocketPeer) ReadMessageData() (interface{}, []byte, error) {
	_, rb, err := p.conn.ReadMessage()
	if err != nil {
		return nil, nil, err
	}
	if len(rb) < 7 {
		return nil, nil, ErrInvalidLength
	}

	t := util.BytesToUint16(rb)
	cps := rb[2:3]
	Len := util.BytesToUint32(rb[3:])
	if Len == 0 {
		return nil, nil, ErrUnknownMessage
	} else if len(rb) != 7+int(Len) {
		return nil, nil, ErrInvalidLength
	} else {
		zbs := rb[7:]
		var bs []byte
		if cps[0] == 1 {
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
		return m, rb, nil
	}
}

// Send sends a message to the WebsocketPeer
func (p *WebsocketPeer) Send(m interface{}) error {
	data, err := MessageToPacket(m)
	if err != nil {
		return err
	}
	if err := p.SendRaw(data); err != nil {
		return err
	}
	return nil
}

// SendRaw sends packet to the WebsocketPeer
func (p *WebsocketPeer) SendRaw(bs []byte) error {
	p.writeQueue.Push(bs)
	return nil
}

// UpdateGuessHeight updates the guess height of the WebsocketPeer
func (p *WebsocketPeer) UpdateGuessHeight(height uint32) {
	p.Lock()
	defer p.Unlock()

	p.guessHeight = height
}

// GuessHeight updates the guess height of the WebsocketPeer
func (p *WebsocketPeer) GuessHeight() uint32 {
	return p.guessHeight
}

// ConnectedTime returns peer connected time
func (p *WebsocketPeer) ConnectedTime() int64 {
	return p.connectedTime
}
