// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
)

// OCFReader structure is used to read Object Container Files (OCF).
type OCFReader struct {
	header *ocfHeader
	block  []byte // buffer from which decoding takes place
	rerr   error  // most recent error that took place while reading bytes (unrecoverable)
	ior    io.Reader
	// cr counts the bytes consumed from the original input stream, which lets
	// block receipts report offsets without rescanning the stream.
	cr                  *blockCountingReader
	readReady           bool  // true after Scan and before Read
	remainingBlockItems int64 // count of encoded data items remaining in block buffer to be decoded
	receipts            []*BlockReceipt
}

// NewOCFReader initializes and returns a new structure used to read an Avro
// Object Container File (OCF).
//
// Offsets reported by BlockReceipts are byte positions in the stream provided
// here. When exact file offsets matter, pass the original stream (for example
// an *os.File) rather than a reader that performs speculative readahead such
// as bufio.Reader; a bufio.Reader may have consumed bytes beyond the current
// block, making reported end offsets point past the block rather than at it.
//
//	func example(ior io.Reader) error {
//	    // NOTE: Wrap provided io.Reader in a buffered reader, which improves the
//	    // performance of streaming file data.
//	    br := bufio.NewReader(ior)
//	    ocfr, err := goavro.NewOCFReader(br)
//	    if err != nil {
//	        return err
//	    }
//	    for ocfr.Scan() {
//	        datum, err := ocfr.Read()
//	        if err != nil {
//	            return err
//	        }
//	        fmt.Println(datum)
//	    }
//	    return ocfr.Err()
//	}
func NewOCFReader(ior io.Reader) (*OCFReader, error) {
	cr := &blockCountingReader{r: ior}
	header, err := readOCFHeader(cr)
	if err != nil {
		return nil, fmt.Errorf("cannot create OCFReader: %s", err)
	}
	return &OCFReader{header: header, ior: cr, cr: cr}, nil
}

// MetaData returns the file metadata map found within the OCF file
func (ocfr *OCFReader) MetaData() map[string][]byte {
	return ocfr.header.metadata
}

// Codec returns the codec found within the OCF file.
func (ocfr *OCFReader) Codec() *Codec {
	return ocfr.header.codec
}

// CompressionName returns the name of the compression algorithm found within
// the OCF file.
func (ocfr *OCFReader) CompressionName() string {
	switch ocfr.header.compressionID {
	case compressionNull:
		return CompressionNullLabel
	case compressionDeflate:
		return CompressionDeflateLabel
	case compressionSnappy:
		return CompressionSnappyLabel
	default:
		return "should not get here: unrecognized compression algorithm"
	}
}

// Err returns the last error encountered while reading the OCF file.  See
// `NewOCFReader` documentation for an example.
func (ocfr *OCFReader) Err() error {
	return ocfr.rerr
}

// Read consumes one datum value from the Avro OCF stream and returns it. Read
// is designed to be called only once after each invocation of the Scan method.
// See `NewOCFReader` documentation for an example.
func (ocfr *OCFReader) Read() (interface{}, error) {
	// NOTE: Test previous error before testing readReady to prevent overwriting
	// previous error.
	if ocfr.rerr != nil {
		return nil, ocfr.rerr
	}
	if !ocfr.readReady {
		ocfr.rerr = errors.New("Read called without successful Scan")
		return nil, ocfr.rerr
	}
	ocfr.readReady = false

	// If the block buffer is exhausted while the frame claimed to hold more
	// datum values, the declared block count was inflated. This is a framing
	// count error rather than a codec error, and the block is not complete.
	if len(ocfr.block) == 0 && ocfr.remainingBlockItems > 0 {
		err := fmt.Errorf("block count mismatch: block declared %d items, but only %d items could be decoded",
			ocfr.currentReceipt().DeclaredItems, ocfr.currentReceipt().DecodedItems)
		ocfr.failBlock(BlockErrorCount, err)
		return nil, err
	}

	// decode one datum value from block
	var datum interface{}
	datum, ocfr.block, ocfr.rerr = ocfr.header.codec.NativeFromBinary(ocfr.block)
	if ocfr.rerr != nil {
		ocfr.failBlock(BlockErrorCodec, fmt.Errorf("cannot decode datum: %s", ocfr.rerr))
		return false, ocfr.rerr
	}
	ocfr.remainingBlockItems--

	if b := ocfr.currentReceipt(); b != nil {
		b.DecodedItems++
		// A block is fully accounted for only when every declared datum
		// value has been decoded with no bytes left over before the sync
		// marker. Any later discrepancy clears Complete again.
		if ocfr.remainingBlockItems == 0 && len(ocfr.block) == 0 {
			b.Complete = true
		}
	}

	return datum, nil
}

// RemainingBlockItems returns the number of items remaining in the block being
// processed.
func (ocfr *OCFReader) RemainingBlockItems() int64 {
	return ocfr.remainingBlockItems
}

// BlockReceipts returns a copy of the explicit receipts for every block frame
// the reader has started so far, in stream order. A receipt is appended as
// soon as a block frame begins and is never removed, so blocks delivered
// before a later failure remain recorded and are not replayed. Each receipt
// is populated incrementally while the block is streamed and decoded; the
// reader never decompresses or rescans a block just to compute statistics.
func (ocfr *OCFReader) BlockReceipts() []BlockReceipt {
	out := make([]BlockReceipt, len(ocfr.receipts))
	for i, b := range ocfr.receipts {
		out[i] = *b
	}
	return out
}

// Scan returns true when there is at least one more data item to be read from
// the Avro OCF. Scan ought to be called prior to calling the Read method each
// time the Read method is invoked.  See `NewOCFReader` documentation for an
// example.
func (ocfr *OCFReader) Scan() bool {
	ocfr.readReady = false

	if ocfr.rerr != nil {
		return false
	}

	// NOTE: If there are no more remaining data items from the existing block,
	// then attempt to slurp in the next block.
	if ocfr.remainingBlockItems <= 0 {
		if count := len(ocfr.block); count != 0 {
			// The producer claimed a smaller block count than the block
			// actually holds: decoded datum values did not consume it all.
			err := fmt.Errorf("block count mismatch: %d extra bytes between final datum and block sync marker", count)
			return ocfr.failBlock(BlockErrorCount, err)
		}

		// Open the block receipt before consuming any byte of its frame so
		// DataOffset anchors at the first byte of the block count long.
		b := ocfr.beginBlockReceipt()

		// Read the block count and update the number of remaining items for
		// this block.
		ocfr.remainingBlockItems, ocfr.rerr = longBinaryReader(ocfr.ior)
		if ocfr.rerr != nil {
			if ocfr.rerr == io.EOF && b.DataOffset == ocfr.cr.consumed {
				// clean end of file: the block frame never started, so drop
				// the empty receipt and report ordinary completion.
				ocfr.receipts = ocfr.receipts[:len(ocfr.receipts)-1]
				ocfr.rerr = nil
				return false
			}
			if ocfr.rerr == io.EOF || ocfr.rerr == io.ErrUnexpectedEOF {
				return ocfr.failBlock(BlockErrorTruncated, fmt.Errorf("cannot read block count: %s", ocfr.rerr))
			}
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot read block count: %s", ocfr.rerr))
		}

		b.DeclaredItems = ocfr.remainingBlockItems
		if ocfr.remainingBlockItems <= 0 {
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot decode when block count is not greater than 0: %d", ocfr.remainingBlockItems))
		}
		if ocfr.remainingBlockItems > MaxBlockCount {
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot decode when block count exceeds MaxBlockCount: %d > %d", ocfr.remainingBlockItems, MaxBlockCount))
		}

		var blockSize int64
		blockSize, ocfr.rerr = longBinaryReader(ocfr.ior)
		if ocfr.rerr != nil {
			if ocfr.rerr == io.EOF || ocfr.rerr == io.ErrUnexpectedEOF {
				return ocfr.failBlock(BlockErrorTruncated, fmt.Errorf("cannot read block size: %s", ocfr.rerr))
			}
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot read block size: %s", ocfr.rerr))
		}
		if blockSize <= 0 {
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot decode when block size is not greater than 0: %d", blockSize))
		}
		if blockSize > MaxBlockSize {
			return ocfr.failBlock(BlockErrorFraming, fmt.Errorf("cannot decode when block size exceeds MaxBlockSize: %d > %d", blockSize, MaxBlockSize))
		}

		b.CompressedSize = blockSize
		b.PayloadOffset = ocfr.cr.consumed

		// read entire block into buffer
		ocfr.block = make([]byte, blockSize)
		if _, ocfr.rerr = io.ReadFull(ocfr.ior, ocfr.block); ocfr.rerr != nil {
			b.EndOffset = ocfr.cr.consumed
			return ocfr.failBlock(BlockErrorTruncated, fmt.Errorf("cannot read block: %s", ocfr.rerr))
		}

		switch ocfr.header.compressionID {
		case compressionNull:
			b.DecompressedSize = int64(len(ocfr.block))

		case compressionDeflate:
			// NOTE: flate.NewReader wraps with io.ByteReader if argument does
			// not implement that interface.
			rc := flate.NewReader(bytes.NewBuffer(ocfr.block))
			var decoded []byte
			decoded, ocfr.rerr = io.ReadAll(rc)
			if ocfr.rerr != nil {
				_ = rc.Close()
				return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("cannot decompress deflate block: %s", ocfr.rerr))
			}
			if err := rc.Close(); err != nil {
				return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("cannot decompress deflate block: %s", err))
			}
			ocfr.block = decoded
			b.DecompressedSize = int64(len(decoded))

		case compressionSnappy:
			index := len(ocfr.block) - 4 // last 4 bytes is crc32 of decoded block
			if index <= 0 {
				return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("cannot decompress snappy without CRC32 checksum: %d", len(ocfr.block)))
			}
			decoded, err := snappy.Decode(nil, ocfr.block[:index])
			if err != nil {
				return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("cannot decompress snappy block: %s", err))
			}
			actualCRC := crc32.ChecksumIEEE(decoded)
			expectedCRC := binary.BigEndian.Uint32(ocfr.block[index : index+4])
			if actualCRC != expectedCRC {
				return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("snappy CRC32 checksum mismatch: %x != %x", actualCRC, expectedCRC))
			}
			ocfr.block = decoded
			b.DecompressedSize = int64(len(decoded))

		default:
			return ocfr.failBlock(BlockErrorCodec, fmt.Errorf("should not get here: cannot decompress block using unrecognized compression: %d", ocfr.header.compressionID))

		}

		// read and ensure sync marker matches
		sync := make([]byte, ocfSyncLength)
		b.SyncOffset = ocfr.cr.consumed
		var n int
		if n, ocfr.rerr = io.ReadFull(ocfr.ior, sync); ocfr.rerr != nil {
			b.EndOffset = ocfr.cr.consumed
			return ocfr.failBlock(BlockErrorTruncated, fmt.Errorf("cannot read sync marker: read %d out of %d bytes: %s", n, ocfSyncLength, ocfr.rerr))
		}
		if !bytes.Equal(sync, ocfr.header.syncMarker[:]) {
			b.EndOffset = ocfr.cr.consumed
			return ocfr.failBlock(BlockErrorSync, fmt.Errorf("sync marker mismatch: %v != %v", sync, ocfr.header.syncMarker))
		}
		b.SyncVerified = true
		b.EndOffset = ocfr.cr.consumed
		// Complete is finalized later, while datum values are decoded, since
		// it also requires the decoded count to match the declared count.
	}

	ocfr.readReady = true
	return true
}

// SkipThisBlockAndReset can be called after an error occurs while reading or
// decoding datum values from an OCF stream. OCF specifies each OCF stream
// contain one or more blocks of data. Each block consists of a block count, the
// number of bytes for the block, followed be the possibly compressed
// block. Inside each decompressed block is all of the binary encoded datum
// values concatenated together. In other words, OCF framing is at a block level
// rather than a datum level. If there is an error while reading or decoding a
// datum, the reader is not able to skip to the next datum value, because OCF
// does not have any markers for where each datum ends and the next one
// begins. Therefore, the reader is only able to skip this datum value and all
// subsequent datum values in the current block, move to the next block and
// start decoding datum values there.
func (ocfr *OCFReader) SkipThisBlockAndReset() {
	// ??? is it an error to call method unless the reader has had an error
	// The failed block's receipt is retained as a historical record and is
	// never rewritten or replayed; only the in-flight decode state resets.
	ocfr.remainingBlockItems = 0
	ocfr.block = ocfr.block[:0]
	ocfr.rerr = nil
}
