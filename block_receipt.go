// Copyright [2026] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import "io"

// BlockErrorStage identifies the stage at which a block failed while being
// read from an OCF stream. It lets callers tell apart, for instance, a
// truncated block payload from a codec error while decoding its datum values.
type BlockErrorStage int

const (
	// BlockErrorNone means the block has not encountered an error.
	BlockErrorNone BlockErrorStage = iota

	// BlockErrorTruncated means the file ended before every byte of the
	// block frame (count, size, payload, or sync marker) could be read.
	BlockErrorTruncated

	// BlockErrorFraming means the declared block count or block size was
	// invalid (zero, negative, or larger than the permitted maximum).
	BlockErrorFraming

	// BlockErrorCodec means the compression codec refused the block payload
	// (corrupt deflate or snappy data or a snappy checksum mismatch).
	BlockErrorCodec

	// BlockErrorSync means the 16-byte sync marker following the block did
	// not match the sync marker stored in the OCF header.
	BlockErrorSync

	// BlockErrorCount means the number of datum values that could actually
	// be decoded from the block did not agree with the block count declared
	// in the frame, or extra bytes were left over inside the block.
	BlockErrorCount
)

// String returns a short, stable label for the error stage.
func (s BlockErrorStage) String() string {
	switch s {
	case BlockErrorNone:
		return "none"
	case BlockErrorTruncated:
		return "truncated"
	case BlockErrorFraming:
		return "framing"
	case BlockErrorCodec:
		return "codec"
	case BlockErrorSync:
		return "sync"
	case BlockErrorCount:
		return "count"
	default:
		return "unknown"
	}
}

// BlockReceipt is an explicit receipt for one OCF block frame encountered by
// an OCFReader. Every block frame the reader starts produces a receipt, even
// when the block turns out to be corrupt: inspect Err, Stage, and Complete to
// decide whether the block was usable.
//
// All offsets are byte offsets measured in the original, uncompressed input
// stream as supplied to NewOCFReader. Statistics are collected while the
// block is streamed and decoded; the block is never decompressed or decoded a
// second time merely to fill in a receipt.
type BlockReceipt struct {
	// Index is the zero-based ordinal of this block within the stream.
	Index int64

	// DataOffset is the byte offset in the original input stream at which
	// the block frame begins, i.e. the first byte of the encoded block
	// count long.
	DataOffset int64

	// PayloadOffset is the byte offset in the original input stream at
	// which the (possibly compressed) block payload begins, right after the
	// encoded block count and block size longs.
	PayloadOffset int64

	// SyncOffset is the byte offset of the first byte of the 16-byte sync
	// marker that terminates the block frame. It is zero until the full
	// payload has been read.
	SyncOffset int64

	// EndOffset is the byte offset immediately after the block frame. For a
	// block whose sync marker verified successfully it is the offset where
	// the following block begins (or end of file). It is zero until the
	// frame has been fully consumed.
	EndOffset int64

	// DeclaredItems is the block count read from the frame: the number of
	// datum values the producer claimed to have written.
	DeclaredItems int64

	// DecodedItems is the number of datum values actually decoded and
	// delivered from the block, incremented one at a time by Read.
	DecodedItems int64

	// CompressedSize is the block size read from the frame: the number of
	// on-disk payload bytes, still compressed when a compression codec is in
	// use.
	CompressedSize int64

	// DecompressedSize is the number of payload bytes after the compression
	// codec ran. For the "null" codec it equals CompressedSize.
	DecompressedSize int64

	// SyncVerified is true only after the 16 bytes following the payload
	// were found to equal the header sync marker.
	SyncVerified bool

	// Complete is true only after the block was fully accounted for: the
	// frame was read, the codec (if any) ran, the sync marker verified, and
	// every declared datum value was decoded with no extra bytes left over.
	// A corrupt or truncated block is never marked complete, even when some
	// of its datum values were already delivered.
	Complete bool

	// Stage reports where in block processing the error below arose. It is
	// BlockErrorNone while no error has been recorded for this block.
	Stage BlockErrorStage

	// Err is the error attributed to this block, or nil when the block has
	// not failed.
	Err error
}

// blockCountingReader wraps an io.Reader and keeps track of the number of
// bytes consumed from it, so receipts can report offsets in the original
// stream without buffering or rescanning the file.
type blockCountingReader struct {
	r        io.Reader
	consumed int64
}

func (cr *blockCountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.consumed += int64(n)
	return n, err
}

// beginBlockReceipt opens a receipt for the next block frame, anchored at the
// current position of the underlying input stream.
func (ocfr *OCFReader) beginBlockReceipt() *BlockReceipt {
	offset := ocfr.cr.consumed
	b := &BlockReceipt{
		Index:      int64(len(ocfr.receipts)),
		DataOffset: offset,
	}
	ocfr.receipts = append(ocfr.receipts, b)
	return b
}

// currentReceipt returns the receipt for the block presently being processed.
func (ocfr *OCFReader) currentReceipt() *BlockReceipt {
	if len(ocfr.receipts) == 0 {
		return nil
	}
	return ocfr.receipts[len(ocfr.receipts)-1]
}

// failBlock attributes an error to the receipt for the block in flight and
// stashes it as the reader's sticky error. Complete is left false.
func (ocfr *OCFReader) failBlock(stage BlockErrorStage, err error) bool {
	if b := ocfr.currentReceipt(); b != nil {
		b.Stage = stage
		b.Err = err
	}
	ocfr.rerr = err
	return false
}
