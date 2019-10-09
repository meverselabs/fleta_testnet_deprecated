package peer

// Peer manages send and recv of the connection
type Peer interface {
	ID() string
	Name() string
	Close()
	ReadMessageData() (interface{}, []byte, error)
	Send(m interface{}) error
	SendRaw(bs []byte) error
	SendPacket(wbs []byte) error
	UpdateGuessHeight(height uint32)
	GuessHeight() uint32
	ConnectedTime() int64
}
