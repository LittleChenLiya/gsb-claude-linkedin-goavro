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
	"errors"
	"fmt"
	"testing"
)

// testReceiptSyncMarker is a fixed sync marker so the tests and the example
// below are deterministic.
var testReceiptSyncMarker = [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

// buildTwoBlockOCF returns a small in-memory OCF stream with exactly two
// blocks: the first block holds the longs 1 and 2, the second holds 3, 4
// and 5.
func buildTwoBlockOCF(t *testing.T, compressionName string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewOCFWriter(OCFConfig{
		W:               &buf,
		Schema:          `"long"`,
		CompressionName: compressionName,
		SyncMarker:      testReceiptSyncMarker,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]interface{}{int64(1), int64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]interface{}{int64(3), int64(4), int64(5)}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// firstBlockOffset returns the offset of the first block by learning it from
// a clean read pass, so tests do not hard-code the header length.
func firstBlockOffset(t *testing.T, blob []byte) int64 {
	t.Helper()
	ocfr, err := NewOCFReader(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	if !ocfr.Scan() {
		t.Fatalf("cannot scan first block: %s", ocfr.Err())
	}
	receipts := ocfr.Receipts()
	if len(receipts) != 1 {
		t.Fatalf("expected 1 in-progress receipt, got %d", len(receipts))
	}
	return receipts[0].Offset
}

// The two-block manifest: every receipt field is accurate, and the byte
// ranges are expressed in original file bytes.
func TestOCFReaderBlockReceiptsTwoBlocks(t *testing.T) {
	for _, compressionName := range []string{CompressionNullLabel, CompressionDeflateLabel, CompressionSnappyLabel} {
		t.Run(compressionName, func(t *testing.T) {
			blob := buildTwoBlockOCF(t, compressionName)

			ocfr, err := NewOCFReader(bytes.NewReader(blob))
			if err != nil {
				t.Fatal(err)
			}
			var values []interface{}
			for ocfr.Scan() {
				datum, err := ocfr.Read()
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, datum)
			}
			if err := ocfr.Err(); err != nil {
				t.Fatal(err)
			}
			if got, want := fmt.Sprint(values), "[1 2 3 4 5]"; got != want {
				t.Fatalf("got %s, want %s", got, want)
			}

			receipts := ocfr.Receipts()
			if got, want := len(receipts), 2; got != want {
				t.Fatalf("got %d receipts, want %d", got, want)
			}

			r0, r1 := receipts[0], receipts[1]

			// record counts, sync verification, completion
			if r0.Index != 0 || r1.Index != 1 {
				t.Errorf("bad indices: %d, %d", r0.Index, r1.Index)
			}
			if r0.DeclaredCount != 2 || r0.DecodedCount != 2 {
				t.Errorf("block 0 counts: declared %d decoded %d", r0.DeclaredCount, r0.DecodedCount)
			}
			if r1.DeclaredCount != 3 || r1.DecodedCount != 3 {
				t.Errorf("block 1 counts: declared %d decoded %d", r1.DeclaredCount, r1.DecodedCount)
			}
			for i, r := range receipts {
				if !r.SyncVerified {
					t.Errorf("block %d: sync not verified", i)
				}
				if !r.Complete {
					t.Errorf("block %d: not marked complete", i)
				}
				if r.Err != nil {
					t.Errorf("block %d: unexpected error: %s", i, r.Err)
				}
			}

			// compressed sizes come from the block headers, and the block
			// byte ranges tile the original file exactly
			if compressionName == CompressionNullLabel {
				if r0.CompressedSize != 2 || r1.CompressedSize != 3 {
					t.Errorf("null codec compressed sizes: %d, %d", r0.CompressedSize, r1.CompressedSize)
				}
			}
			if r0.Offset <= 0 {
				t.Errorf("block 0 offset should be past the header: %d", r0.Offset)
			}
			if r0.Offset+r0.ByteSize != r1.Offset {
				t.Errorf("block ranges not contiguous: [%d,%d) then [%d,%d)",
					r0.Offset, r0.Offset+r0.ByteSize, r1.Offset, r1.Offset+r1.ByteSize)
			}
			if r1.Offset+r1.ByteSize != int64(len(blob)) {
				t.Errorf("block 1 ends at %d, file is %d bytes", r1.Offset+r1.ByteSize, len(blob))
			}
			// the count varint at each block offset matches the declared
			// count, proving offsets refer to original file bytes
			if blob[r0.Offset] != 0x04 {
				t.Errorf("byte at block 0 offset: %#x, want 0x04 (count 2)", blob[r0.Offset])
			}
			if blob[r1.Offset] != 0x06 {
				t.Errorf("byte at block 1 offset: %#x, want 0x06 (count 3)", blob[r1.Offset])
			}

			// Receipts returns a copy: mutating it does not affect the reader
			receipts[0].Complete = false
			if !ocfr.Receipts()[0].Complete {
				t.Error("Receipts does not return a copy")
			}
		})
	}
}

// A corrupted sync marker on the second block damages only the second block:
// the first block is fully delivered and its receipt stays complete.
func TestOCFReaderBlockReceiptSecondBlockSyncCorrupted(t *testing.T) {
	blob := buildTwoBlockOCF(t, CompressionNullLabel)
	blob[len(blob)-1] ^= 0xff // last byte of the second block sync marker

	ocfr, err := NewOCFReader(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	var values []interface{}
	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, datum)
	}
	if got, want := fmt.Sprint(values), "[1 2]"; got != want {
		t.Fatalf("first block values: got %s, want %s", got, want)
	}
	if err := ocfr.Err(); !errors.Is(err, ErrOCFBlockSync) {
		t.Fatalf("got error %v, want ErrOCFBlockSync", err)
	}

	receipts := ocfr.Receipts()
	if len(receipts) != 2 {
		t.Fatalf("got %d receipts, want 2", len(receipts))
	}
	r0, r1 := receipts[0], receipts[1]
	if !r0.Complete || r0.Err != nil || r0.DecodedCount != 2 {
		t.Errorf("block 0 should be complete and intact: %+v", r0)
	}
	if r1.Complete {
		t.Error("block 1 must not be marked complete")
	}
	if r1.SyncVerified {
		t.Error("block 1 sync must not be verified")
	}
	if !errors.Is(r1.Err, ErrOCFBlockSync) {
		t.Errorf("block 1 error: got %v, want ErrOCFBlockSync", r1.Err)
	}
	if r1.DeclaredCount != 3 || r1.DecodedCount != 0 {
		t.Errorf("block 1 counts: declared %d decoded %d", r1.DeclaredCount, r1.DecodedCount)
	}
}

// A falsified declared record count is discovered when the block ends.
func TestOCFReaderBlockReceiptDeclaredCountFalsified(t *testing.T) {
	t.Run("declared smaller than actual", func(t *testing.T) {
		blob := buildTwoBlockOCF(t, CompressionNullLabel)
		off := firstBlockOffset(t, blob)
		blob[off] = 0x02 // block 0 now declares 1 record but encodes 2

		ocfr, err := NewOCFReader(bytes.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		var values []interface{}
		for ocfr.Scan() {
			datum, err := ocfr.Read()
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, datum)
		}
		if got, want := fmt.Sprint(values), "[1]"; got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
		// discovered at block end: one undecoded byte remains in the block
		if err := ocfr.Err(); !errors.Is(err, ErrOCFBlockCountMismatch) {
			t.Fatalf("got error %v, want ErrOCFBlockCountMismatch", err)
		}
		r0 := ocfr.Receipts()[0]
		if r0.Complete {
			t.Error("block 0 must not be marked complete")
		}
		if r0.DeclaredCount != 1 || r0.DecodedCount != 1 {
			t.Errorf("block 0 counts: declared %d decoded %d", r0.DeclaredCount, r0.DecodedCount)
		}
		if !errors.Is(r0.Err, ErrOCFBlockCountMismatch) {
			t.Errorf("block 0 error: got %v, want ErrOCFBlockCountMismatch", r0.Err)
		}
	})

	t.Run("declared larger than actual", func(t *testing.T) {
		blob := buildTwoBlockOCF(t, CompressionNullLabel)
		off := firstBlockOffset(t, blob)
		blob[off] = 0x06 // block 0 now declares 3 records but encodes 2

		ocfr, err := NewOCFReader(bytes.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		var values []interface{}
		var readErr error
		for ocfr.Scan() {
			var datum interface{}
			if datum, readErr = ocfr.Read(); readErr != nil {
				break
			}
			values = append(values, datum)
		}
		if got, want := fmt.Sprint(values), "[1 2]"; got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
		// discovered when the block data is exhausted at the end of the block
		if !errors.Is(readErr, ErrOCFBlockCountMismatch) {
			t.Fatalf("got error %v, want ErrOCFBlockCountMismatch", readErr)
		}
		r0 := ocfr.Receipts()[0]
		if r0.Complete {
			t.Error("block 0 must not be marked complete")
		}
		if r0.DeclaredCount != 3 || r0.DecodedCount != 2 {
			t.Errorf("block 0 counts: declared %d decoded %d", r0.DeclaredCount, r0.DecodedCount)
		}
	})
}

// Block truncation and codec failures are attributed to different causes.
func TestOCFReaderBlockReceiptTruncatedVersusCodec(t *testing.T) {
	t.Run("truncated", func(t *testing.T) {
		blob := buildTwoBlockOCF(t, CompressionNullLabel)
		truncated := blob[:len(blob)-10] // cut inside the second sync marker

		ocfr, err := NewOCFReader(bytes.NewReader(truncated))
		if err != nil {
			t.Fatal(err)
		}
		for ocfr.Scan() {
			if _, err := ocfr.Read(); err != nil {
				t.Fatal(err)
			}
		}
		err = ocfr.Err()
		if !errors.Is(err, ErrOCFBlockTruncated) {
			t.Fatalf("got error %v, want ErrOCFBlockTruncated", err)
		}
		if errors.Is(err, ErrOCFBlockCodec) {
			t.Fatalf("truncation must not be attributed to the codec: %v", err)
		}
		receipts := ocfr.Receipts()
		if len(receipts) != 2 {
			t.Fatalf("got %d receipts, want 2", len(receipts))
		}
		if !receipts[0].Complete {
			t.Error("block 0 should be complete")
		}
		if receipts[1].Complete || !errors.Is(receipts[1].Err, ErrOCFBlockTruncated) {
			t.Errorf("block 1 should be incomplete with truncation error: %+v", receipts[1])
		}
	})

	t.Run("codec", func(t *testing.T) {
		blob := buildTwoBlockOCF(t, CompressionSnappyLabel)

		// learn the block ranges from a clean pass, then corrupt the CRC32
		// checksum stored at the tail of the second block data
		ocfr, err := NewOCFReader(bytes.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		for ocfr.Scan() {
			if _, err := ocfr.Read(); err != nil {
				t.Fatal(err)
			}
		}
		r1 := ocfr.Receipts()[1]
		crcOffset := r1.Offset + r1.ByteSize - ocfSyncLength - 1
		blob[crcOffset] ^= 0xff

		ocfr, err = NewOCFReader(bytes.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		for ocfr.Scan() {
			if _, err := ocfr.Read(); err != nil {
				t.Fatal(err)
			}
		}
		err = ocfr.Err()
		if !errors.Is(err, ErrOCFBlockCodec) {
			t.Fatalf("got error %v, want ErrOCFBlockCodec", err)
		}
		if errors.Is(err, ErrOCFBlockTruncated) {
			t.Fatalf("codec failure must not be attributed to truncation: %v", err)
		}
		receipts := ocfr.Receipts()
		if !receipts[0].Complete {
			t.Error("block 0 should be complete")
		}
		if receipts[1].Complete || !errors.Is(receipts[1].Err, ErrOCFBlockCodec) {
			t.Errorf("block 1 should be incomplete with codec error: %+v", receipts[1])
		}
	})
}

// Receipts are append-only: delivered blocks are never replayed, and skipping
// a broken block continues with the next block without duplicating receipts.
func TestOCFReaderBlockReceiptsNoReplay(t *testing.T) {
	blob := buildTwoBlockOCF(t, CompressionNullLabel)
	off := firstBlockOffset(t, blob)
	blob[off] = 0x06 // block 0 declares 3 records but encodes 2

	ocfr, err := NewOCFReader(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	var values []interface{}
	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			// skip the broken block and keep reading the next one
			ocfr.SkipThisBlockAndReset()
			continue
		}
		values = append(values, datum)
	}
	if err := ocfr.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(values), "[1 2 3 4 5]"; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}

	receipts := ocfr.Receipts()
	if len(receipts) != 2 {
		t.Fatalf("got %d receipts, want 2 (delivered blocks must not be replayed)", len(receipts))
	}
	r0, r1 := receipts[0], receipts[1]
	if r0.Complete || !errors.Is(r0.Err, ErrOCFBlockCountMismatch) {
		t.Errorf("block 0 should be incomplete with count mismatch: %+v", r0)
	}
	if r0.DeclaredCount != 3 || r0.DecodedCount != 2 {
		t.Errorf("block 0 counts: declared %d decoded %d", r0.DeclaredCount, r0.DecodedCount)
	}
	if !r1.Complete || r1.DecodedCount != 3 {
		t.Errorf("block 1 should be complete with 3 decoded records: %+v", r1)
	}

	// further Scan calls do not append or alter receipts
	ocfr.Scan()
	again := ocfr.Receipts()
	if len(again) != 2 || again[0] != receipts[0] || again[1] != receipts[1] {
		t.Error("receipts changed after the stream was exhausted")
	}
}

// ExampleOCFReader_Receipts demonstrates the explicit per-block receipts on a
// small in-memory OCF stream with two blocks.
func ExampleOCFReader_Receipts() {
	var buf bytes.Buffer
	w, err := NewOCFWriter(OCFConfig{
		W:          &buf,
		Schema:     `"long"`,
		SyncMarker: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	// each Append writes one block
	if err := w.Append([]interface{}{int64(1), int64(2)}); err != nil {
		fmt.Println(err)
		return
	}
	if err := w.Append([]interface{}{int64(3), int64(4), int64(5)}); err != nil {
		fmt.Println(err)
		return
	}

	ocfr, err := NewOCFReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		fmt.Println(err)
		return
	}
	for ocfr.Scan() {
		if _, err := ocfr.Read(); err != nil {
			fmt.Println(err)
			return
		}
	}
	if err := ocfr.Err(); err != nil {
		fmt.Println(err)
		return
	}
	for _, r := range ocfr.Receipts() {
		fmt.Printf("block %d: range=[%d,%d) declared=%d decoded=%d compressed=%d sync=%v complete=%v err=%v\n",
			r.Index, r.Offset, r.Offset+r.ByteSize, r.DeclaredCount, r.DecodedCount,
			r.CompressedSize, r.SyncVerified, r.Complete, r.Err)
	}
	// Output:
	// block 0: range=[57,77) declared=2 decoded=2 compressed=2 sync=true complete=true err=<nil>
	// block 1: range=[77,98) declared=3 decoded=3 compressed=3 sync=true complete=true err=<nil>
}
