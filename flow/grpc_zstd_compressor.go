package flow

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

const zstdCompressorName = "zstd"

func init() {
	enc, _ := zstd.NewWriter(nil, zstd.WithWindowSize(512*1024))
	c := &zstdCompressor{
		encoder: enc,
	}
	c.decoderPool.New = func() interface{} {
		dec, _ := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
		)
		return dec
	}
	encoding.RegisterCompressor(c)
}

type zstdCompressor struct {
	encoder     *zstd.Encoder
	decoderPool sync.Pool
}

func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdWriteCloser{
		enc:    c.encoder,
		writer: w,
	}, nil
}

type zstdWriteCloser struct {
	enc    *zstd.Encoder
	writer io.Writer
	buf    bytes.Buffer
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	compressed := z.enc.EncodeAll(z.buf.Bytes(), nil)
	_, err := io.Copy(z.writer, bytes.NewReader(compressed))
	return err
}

type pooledDecoderReader struct {
	decoder *zstd.Decoder
	pool    *sync.Pool
}

func (r *pooledDecoderReader) Read(p []byte) (int, error) {
	n, err := r.decoder.Read(p)
	if err != nil {
		r.returnToPool()
	}
	return n, err
}

func (r *pooledDecoderReader) returnToPool() {
	if r.decoder != nil {
		r.decoder.Reset(nil)
		r.pool.Put(r.decoder)
		r.decoder = nil
	}
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	decoder := c.decoderPool.Get().(*zstd.Decoder)
	if err := decoder.Reset(r); err != nil {
		c.decoderPool.Put(decoder)
		return nil, err
	}

	return &pooledDecoderReader{
		decoder: decoder,
		pool:    &c.decoderPool,
	}, nil
}

func (c *zstdCompressor) Name() string {
	return zstdCompressorName
}
