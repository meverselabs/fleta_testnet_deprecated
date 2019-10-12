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
	if err == nil {
		cr.buffer.Write(bs[:n])
	}
	return n, err
}

func (cr *CopyReader) Reset() {
	cr.buffer.Reset()
}

func (cr *CopyReader) Bytes() []byte {
	return cr.buffer.Bytes()
}
