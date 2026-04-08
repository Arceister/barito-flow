package flow

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCompressor(&zstdCompressor{})
}

type zstdCompressor struct {
	encoderPool sync.Pool
	decoderPool sync.Pool
}

func (c *zstdCompressor) Name() string {
	return "zstd"
}

func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	enc, ok := c.encoderPool.Get().(*zstd.Encoder)
	if !ok {
		var err error
		enc, err = zstd.NewWriter(nil,
			zstd.WithWindowSize(512*1024),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return nil, err
		}
	}
	enc.Reset(w)
	return &zstdWriteCloser{enc: enc, pool: &c.encoderPool}, nil
}

type zstdWriteCloser struct {
	enc  *zstd.Encoder
	pool *sync.Pool
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.enc.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	err := z.enc.Close()
	z.pool.Put(z.enc)
	return err
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dec, ok := c.decoderPool.Get().(*zstd.Decoder)
	if !ok {
		var err error
		dec, err = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(1<<20), // 1MB max window
		)
		if err != nil {
			return nil, err
		}
	}
	err := dec.Reset(r)
	if err != nil {
		c.decoderPool.Put(dec)
		return nil, err
	}

	return &zstdReadCloser{dec: dec, pool: &c.decoderPool}, nil
}

type zstdReadCloser struct {
	dec  *zstd.Decoder
	pool *sync.Pool
	buf  bytes.Buffer
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	if z.buf.Len() == 0 {
		// Decode all at once, then return from buffer
		_, err := io.Copy(&z.buf, z.dec)
		// Return decoder to pool immediately after full read
		z.dec.Reset(nil)
		z.pool.Put(z.dec)
		z.dec = nil
		if err != nil {
			return 0, err
		}
	}
	return z.buf.Read(p)
}
