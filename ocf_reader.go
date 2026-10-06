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

// Sentinel errors used to attribute OCF block failures to their cause. They
// are wrapped by the error stored in OCFBlockReceipt.Err (and returned by
// OCFReader.Err), so errors.Is may be used to classify a block failure.
var (
	// ErrOCFBlockTruncated indicates the stream ended while reading a block
	// header, the block data, or the block sync marker.
	ErrOCFBlockTruncated = errors.New("ocf block truncated")
	// ErrOCFBlockCodec indicates the block compression codec failed to
	// decompress the block, or its checksum did not match.
	ErrOCFBlockCodec = errors.New("ocf block codec error")
	// ErrOCFBlockCountMismatch indicates the record count declared in the
	// block header does not match the number of records actually decoded
	// from the block data. The mismatch is detected when the block ends:
	// either undecoded bytes remain after the declared records were
	// delivered, or the block data is exhausted before the declared records
	// could be decoded.
	ErrOCFBlockCountMismatch = errors.New("ocf block record count mismatch")
	// ErrOCFBlockSync indicates the sync marker following the block data did
	// not match the sync marker from the OCF header.
	ErrOCFBlockSync = errors.New("ocf block sync marker mismatch")
)

// OCFBlockReceipt is the explicit per-block receipt emitted by OCFReader as
// it consumes an Avro Object Container File. One receipt is created when the
// reader starts loading a block, is updated as records from that block are
// delivered, and is finalized when the block ends. All offsets and sizes are
// expressed in bytes of the original (still compressed) OCF stream, and all
// statistics are collected while reading; the reader never re-decompresses a
// block to compute them.
type OCFBlockReceipt struct {
	// Index is the zero-based sequence number of the block in the stream.
	Index int
	// Offset is the byte offset in the original OCF stream at which the
	// block header (the record count) begins.
	Offset int64
	// ByteSize is the total number of bytes the block occupies in the
	// original OCF stream: record count, block size, (possibly compressed)
	// block data, and the trailing sync marker. The block therefore covers
	// the byte range [Offset, Offset+ByteSize).
	ByteSize int64
	// DeclaredCount is the record count declared by the block header.
	DeclaredCount int64
	// DecodedCount is the number of records actually decoded and delivered
	// from this block.
	DecodedCount int64
	// CompressedSize is the block size declared by the block header, i.e.
	// the number of bytes the (possibly compressed) block data occupies in
	// the stream.
	CompressedSize int64
	// SyncVerified is true when the sync marker following the block data
	// was read and matched the OCF header sync marker.
	SyncVerified bool
	// Complete is true only when the block was read without error, its sync
	// marker verified, and the number of records decoded equals the declared
	// record count. A block that failed for any reason is never marked
	// complete.
	Complete bool
	// Err holds the block-scoped error when the block could not be read or
	// decoded in full, and nil otherwise.
	Err error

	finalized bool // true once the receipt can no longer change
}

// ocfCountingReader wraps an io.Reader and counts the bytes consumed from it,
// so the OCF reader can attribute every block to its byte range in the
// original stream.
type ocfCountingReader struct {
	r io.Reader
	n int64
}

func (cr *ocfCountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

// ReadByte allows longBinaryReader to use its io.ByteReader fast path while
// still counting every consumed byte.
func (cr *ocfCountingReader) ReadByte() (byte, error) {
	if br, ok := cr.r.(io.ByteReader); ok {
		b, err := br.ReadByte()
		if err == nil {
			cr.n++
		}
		return b, err
	}
	var buf [1]byte
	n, err := cr.r.Read(buf[:])
	cr.n += int64(n)
	if err == nil && n == 0 {
		err = io.ErrNoProgress
	}
	return buf[0], err
}

// OCFReader structure is used to read Object Container Files (OCF).
type OCFReader struct {
	header              *ocfHeader
	block               []byte // buffer from which decoding takes place
	rerr                error  // most recent error that took place while reading bytes (unrecoverable)
	cr                  *ocfCountingReader
	receipts            []OCFBlockReceipt // append-only per-block receipts; delivered blocks are never replayed
	readReady           bool              // true after Scan and before Read
	remainingBlockItems int64             // count of encoded data items remaining in block buffer to be decoded
}

// NewOCFReader initializes and returns a new structure used to read an Avro
// Object Container File (OCF).
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
	cr := &ocfCountingReader{r: ior}
	header, err := readOCFHeader(cr)
	if err != nil {
		return nil, fmt.Errorf("cannot create OCFReader: %s", err)
	}
	return &OCFReader{header: header, cr: cr}, nil
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

// Receipts returns a copy of the per-block receipts accumulated so far. One
// receipt is appended each time the reader starts loading a block, and the
// most recent receipt is updated as records from that block are delivered and
// finalized when the block ends. The list is append-only: a block that has
// been delivered is never replayed, so a finalized receipt never changes and
// no block ever appears twice.
func (ocfr *OCFReader) Receipts() []OCFBlockReceipt {
	receipts := make([]OCFBlockReceipt, len(ocfr.receipts))
	copy(receipts, ocfr.receipts)
	return receipts
}

// currentReceipt returns a pointer to the receipt of the block presently
// being decoded, or nil when no block has been loaded yet.
func (ocfr *OCFReader) currentReceipt() *OCFBlockReceipt {
	if i := len(ocfr.receipts) - 1; i >= 0 {
		return &ocfr.receipts[i]
	}
	return nil
}

// failBlock records a block-scoped error on the current receipt, stores it as
// the reader error, and returns false so Scan can return directly.
func (ocfr *OCFReader) failBlock(err error) bool {
	ocfr.rerr = err
	if r := ocfr.currentReceipt(); r != nil && !r.finalized {
		r.Err = err
		r.finalized = true
	}
	return false
}

// finalizeBlockReceipt verifies the receipt of the block that was just
// exhausted: the number of records actually decoded must equal the declared
// record count, and no undecoded bytes may remain in the block buffer. A
// falsified declared count is therefore discovered when the block ends. A
// block that already carries an error is left incomplete and its error is
// preserved.
func (ocfr *OCFReader) finalizeBlockReceipt() error {
	r := ocfr.currentReceipt()
	if r == nil || r.finalized {
		return nil
	}
	r.finalized = true
	if r.Err != nil {
		return nil // error already recorded; reader already stopped or was reset
	}
	if count := len(ocfr.block); count != 0 {
		r.Err = fmt.Errorf("%w: block %d declared %d records, delivered %d, but %d undecoded bytes remain at block end",
			ErrOCFBlockCountMismatch, r.Index, r.DeclaredCount, r.DecodedCount, count)
		return r.Err
	}
	if r.DecodedCount != r.DeclaredCount {
		r.Err = fmt.Errorf("%w: block %d declared %d records but only %d were decoded",
			ErrOCFBlockCountMismatch, r.Index, r.DeclaredCount, r.DecodedCount)
		return r.Err
	}
	r.Complete = true
	return nil
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

	// decode one datum value from block
	var datum interface{}
	datum, ocfr.block, ocfr.rerr = ocfr.header.codec.NativeFromBinary(ocfr.block)
	if ocfr.rerr != nil {
		derr := ocfr.rerr
		if r := ocfr.currentReceipt(); r != nil && !r.finalized {
			if len(ocfr.block) == 0 {
				// The block data is exhausted while the declared record
				// count demands more records: the declared count was
				// falsified, discovered at the end of the block data.
				derr = fmt.Errorf("%w: block %d declared %d records but its data was exhausted after %d: %s",
					ErrOCFBlockCountMismatch, r.Index, r.DeclaredCount, r.DecodedCount, derr)
			} else {
				derr = fmt.Errorf("cannot decode datum in block %d: %s", r.Index, derr)
			}
			r.Err = derr
			r.finalized = true
		}
		ocfr.rerr = derr
		return nil, ocfr.rerr
	}
	ocfr.remainingBlockItems--
	if r := ocfr.currentReceipt(); r != nil && !r.finalized {
		r.DecodedCount++
	}

	return datum, nil
}

// RemainingBlockItems returns the number of items remaining in the block being
// processed.
func (ocfr *OCFReader) RemainingBlockItems() int64 {
	return ocfr.remainingBlockItems
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
		// Finalize the receipt of the block just exhausted, verifying the
		// declared record count against what was actually decoded.
		if err := ocfr.finalizeBlockReceipt(); err != nil {
			ocfr.rerr = err
			return false
		}

		// Byte offset in the original OCF stream where this block begins.
		blockOffset := ocfr.cr.n

		// Read the block count and update the number of remaining items for
		// this block
		ocfr.remainingBlockItems, ocfr.rerr = longBinaryReader(ocfr.cr)
		if ocfr.rerr != nil {
			if ocfr.rerr == io.EOF {
				ocfr.rerr = nil // merely end of file, rather than error
			} else {
				ocfr.rerr = fmt.Errorf("%w: cannot read block count at offset %d: %s", ErrOCFBlockTruncated, blockOffset, ocfr.rerr)
			}
			return false
		}
		if ocfr.remainingBlockItems <= 0 {
			ocfr.rerr = fmt.Errorf("cannot decode when block count is not greater than 0: %d", ocfr.remainingBlockItems)
			return false
		}
		if ocfr.remainingBlockItems > MaxBlockCount {
			ocfr.rerr = fmt.Errorf("cannot decode when block count exceeds MaxBlockCount: %d > %d", ocfr.remainingBlockItems, MaxBlockCount)
			return false
		}

		var blockSize int64
		blockSize, ocfr.rerr = longBinaryReader(ocfr.cr)
		if ocfr.rerr != nil {
			ocfr.rerr = fmt.Errorf("%w: cannot read block size at offset %d: %s", ErrOCFBlockTruncated, ocfr.cr.n, ocfr.rerr)
			return false
		}
		if blockSize <= 0 {
			ocfr.rerr = fmt.Errorf("cannot decode when block size is not greater than 0: %d", blockSize)
			return false
		}
		if blockSize > MaxBlockSize {
			ocfr.rerr = fmt.Errorf("cannot decode when block size exceeds MaxBlockSize: %d > %d", blockSize, MaxBlockSize)
			return false
		}

		// Append the receipt for this block. All of its statistics are
		// collected from the read path; the block is never re-decompressed
		// to compute them.
		ocfr.receipts = append(ocfr.receipts, OCFBlockReceipt{
			Index:          len(ocfr.receipts),
			Offset:         blockOffset,
			DeclaredCount:  ocfr.remainingBlockItems,
			CompressedSize: blockSize,
		})
		receipt := &ocfr.receipts[len(ocfr.receipts)-1]

		// read entire block into buffer
		ocfr.block = make([]byte, blockSize)
		_, ocfr.rerr = io.ReadFull(ocfr.cr, ocfr.block)
		if ocfr.rerr != nil {
			return ocfr.failBlock(fmt.Errorf("%w: cannot read block %d: %d bytes at offset %d: %s", ErrOCFBlockTruncated, receipt.Index, blockSize, ocfr.cr.n, ocfr.rerr))
		}

		switch ocfr.header.compressionID {
		case compressionNull:
			// no-op

		case compressionDeflate:
			// NOTE: flate.NewReader wraps with io.ByteReader if argument does
			// not implement that interface.
			rc := flate.NewReader(bytes.NewBuffer(ocfr.block))
			ocfr.block, ocfr.rerr = io.ReadAll(rc)
			if ocfr.rerr != nil {
				_ = rc.Close()
				return ocfr.failBlock(fmt.Errorf("%w: cannot decompress deflate block %d: %s", ErrOCFBlockCodec, receipt.Index, ocfr.rerr))
			}
			if ocfr.rerr = rc.Close(); ocfr.rerr != nil {
				return ocfr.failBlock(fmt.Errorf("%w: cannot decompress deflate block %d: %s", ErrOCFBlockCodec, receipt.Index, ocfr.rerr))
			}

		case compressionSnappy:
			index := len(ocfr.block) - 4 // last 4 bytes is crc32 of decoded block
			if index <= 0 {
				return ocfr.failBlock(fmt.Errorf("%w: cannot decompress snappy without CRC32 checksum: %d", ErrOCFBlockCodec, len(ocfr.block)))
			}
			decoded, err := snappy.Decode(nil, ocfr.block[:index])
			if err != nil {
				return ocfr.failBlock(fmt.Errorf("%w: cannot decompress: %s", ErrOCFBlockCodec, err))
			}
			actualCRC := crc32.ChecksumIEEE(decoded)
			expectedCRC := binary.BigEndian.Uint32(ocfr.block[index : index+4])
			if actualCRC != expectedCRC {
				return ocfr.failBlock(fmt.Errorf("%w: snappy CRC32 checksum mismatch: %x != %x", ErrOCFBlockCodec, actualCRC, expectedCRC))
			}
			ocfr.block = decoded

		default:
			return ocfr.failBlock(fmt.Errorf("should not get here: cannot compress block using unrecognized compression: %d", ocfr.header.compressionID))

		}

		// read and ensure sync marker matches
		sync := make([]byte, ocfSyncLength)
		var n int
		if n, ocfr.rerr = io.ReadFull(ocfr.cr, sync); ocfr.rerr != nil {
			return ocfr.failBlock(fmt.Errorf("%w: cannot read sync marker: read %d out of %d bytes: %s", ErrOCFBlockTruncated, n, ocfSyncLength, ocfr.rerr))
		}
		if !bytes.Equal(sync, ocfr.header.syncMarker[:]) {
			return ocfr.failBlock(fmt.Errorf("%w: block %d: %v != %v", ErrOCFBlockSync, receipt.Index, sync, ocfr.header.syncMarker))
		}
		receipt.SyncVerified = true
		receipt.ByteSize = ocfr.cr.n - blockOffset
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
//
// The receipt of the skipped block is finalized and is never marked complete;
// blocks already delivered keep their receipts and are never replayed.
func (ocfr *OCFReader) SkipThisBlockAndReset() {
	if r := ocfr.currentReceipt(); r != nil && !r.finalized {
		r.finalized = true
		if r.Err == nil {
			r.Err = fmt.Errorf("block %d skipped after delivering %d of %d declared records", r.Index, r.DecodedCount, r.DeclaredCount)
		}
	}
	ocfr.remainingBlockItems = 0
	ocfr.block = ocfr.block[:0]
	ocfr.rerr = nil
}
