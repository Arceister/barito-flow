package flow

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

const zstdCompressorName = "zstd"

// compressBufPool pools destination byte slices for EncodeAll to avoid a
// per-call heap allocation (same root cause as IBM/sarama#2964).
var compressBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// writeCloserPool pools zstdWriteCloser structs (and their bytes.Buffer) to
// avoid a per-gRPC-call allocation in Compress().
var writeCloserPool = sync.Pool{
	New: func() interface{} {
		return &zstdWriteCloser{}
	},
}

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
	wc := writeCloserPool.Get().(*zstdWriteCloser)
	wc.enc = c.encoder
	wc.writer = w
	wc.buf.Reset()
	return wc, nil
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
	bp := compressBufPool.Get().(*[]byte)
	dst := (*bp)[:0]
	compressed := z.enc.EncodeAll(z.buf.Bytes(), dst)
	_, err := io.Copy(z.writer, bytes.NewReader(compressed))
	// Return the pooled buffer — reset to zero length but keep capacity.
	*bp = compressed[:0]
	compressBufPool.Put(bp)
	z.enc = nil
	z.writer = nil
	writeCloserPool.Put(z)
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
