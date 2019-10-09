package p2p

import (
	"bytes"
	"io"
)

type CopyReader struct {
	r      io.Reader
	buffer bytes.Buffer
}

func NewCopyReader(r io.Reader) *CopyReader {
	return &CopyReader{
		r: r,
	}
}

func (cr *CopyReader) Read(bs []byte) (int, error) {
	n, err := cr.r.Read(bs)
	cr.buffer.Write(bs)
	return n, err
}

func (cr *CopyReader) Bytes() []byte {
	return cr.buffer.Bytes()
}
