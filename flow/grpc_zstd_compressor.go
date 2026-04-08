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
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	encoding.RegisterCompressor(&zstdCompressor{
		encoder: enc,
		decoder: dec,
	})
}

type zstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
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

var decompressBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	buf := decompressBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	if _, err := io.Copy(buf, r); err != nil {
		decompressBufPool.Put(buf)
		return nil, err
	}

	decompressed, err := c.decoder.DecodeAll(buf.Bytes(), nil)
	decompressBufPool.Put(buf)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(decompressed), nil
}

func (c *zstdCompressor) Name() string {
	return zstdCompressorName
}
