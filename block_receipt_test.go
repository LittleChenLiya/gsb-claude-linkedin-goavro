// Copyright [2026] LinkedIn Corp. Licensed under the Apache License, Version
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
	"strings"
	"testing"
)

// testSyncMarker is a fixed, well-known 16-byte OCF sync marker used by the
// receipt tests so corrupting a marker byte is straightforward.
var testSyncMarker = [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

const schemaLong = "{\"type\":\"long\"}"

// buildTwoBlockOCF writes a small in-memory OCF file containing two blocks:
// block 0 holds three long values {13, 42, -12}, block 1 holds two long
// values {-1234, 7}.
func buildTwoBlockOCF(t *testing.T, compression string) []byte {
	t.Helper()
	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:               bb,
		Schema:          schemaLong,
		CompressionName: compression,
		SyncMarker:      testSyncMarker,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = ocfw.Append([]int64{13, 42, -12}); err != nil {
		t.Fatal(err)
	}
	if err = ocfw.Append([]int64{-1234, 7}); err != nil {
		t.Fatal(err)
	}
	return bb.Bytes()
}

// readAllRecords drives the Scan/Read loop and returns delivered values,
// stopping at the first error.
func readAllRecords(ocfr *OCFReader) ([]int64, error) {
	var values []int64
	for ocfr.Scan() {
		value, err := ocfr.Read()
		if err != nil {
			return values, err
		}
		values = append(values, value.(int64))
	}
	return values, ocfr.Err()
}

func TestBlockReceiptsTwoBlocksAccurate(t *testing.T) {
	data := buildTwoBlockOCF(t, CompressionNullLabel)

	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	values, err := readAllRecords(ocfr)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got, want := len(values), 5; got != want {
		t.Fatalf("delivered records: GOT %d; WANT %d", got, want)
	}

	receipts := ocfr.BlockReceipts()
	if got, want := len(receipts), 2; got != want {
		t.Fatalf("receipt count: GOT %d; WANT %d", got, want)
	}

	headerLen := int64(bytes.Index(data, testSyncMarker[:])) + ocfSyncLength

	want := []BlockReceipt{
		{Index: 0, DeclaredItems: 3, DecodedItems: 3, CompressedSize: 3, DecompressedSize: 3},
		{Index: 1, DeclaredItems: 2, DecodedItems: 2, CompressedSize: 3, DecompressedSize: 3},
	}
	for i, got := range receipts {
		if got.Index != want[i].Index {
			t.Errorf("block %d Index: GOT %d; WANT %d", i, got.Index, want[i].Index)
		}
		if got.DeclaredItems != want[i].DeclaredItems {
			t.Errorf("block %d DeclaredItems: GOT %d; WANT %d", i, got.DeclaredItems, want[i].DeclaredItems)
		}
		if got.DecodedItems != want[i].DecodedItems {
			t.Errorf("block %d DecodedItems: GOT %d; WANT %d", i, got.DecodedItems, want[i].DecodedItems)
		}
		if got.CompressedSize != want[i].CompressedSize {
			t.Errorf("block %d CompressedSize: GOT %d; WANT %d", i, got.CompressedSize, want[i].CompressedSize)
		}
		if got.DecompressedSize != want[i].DecompressedSize {
			t.Errorf("block %d DecompressedSize: GOT %d; WANT %d", i, got.DecompressedSize, want[i].DecompressedSize)
		}
		if !got.SyncVerified {
			t.Errorf("block %d SyncVerified: GOT false; WANT true", i)
		}
		if !got.Complete {
			t.Errorf("block %d Complete: GOT false; WANT true", i)
		}
		if got.Stage != BlockErrorNone || got.Err != nil {
			t.Errorf("block %d should have no error, GOT stage=%s err=%v", i, got.Stage, got.Err)
		}
		// offsets are measured in original file bytes, frames are contiguous
		if got.DataOffset != headerLen {
			t.Errorf("block %d DataOffset: GOT %d; WANT %d (original file bytes)", i, got.DataOffset, headerLen)
		}
		if got.PayloadOffset != got.DataOffset+2 {
			t.Errorf("block %d PayloadOffset: GOT %d; WANT DataOffset+2=%d", i, got.PayloadOffset, got.DataOffset+2)
		}
		if got.SyncOffset != got.PayloadOffset+got.CompressedSize {
			t.Errorf("block %d SyncOffset: GOT %d; WANT %d", i, got.SyncOffset, got.PayloadOffset+got.CompressedSize)
		}
		if got.EndOffset != got.SyncOffset+ocfSyncLength {
			t.Errorf("block %d EndOffset: GOT %d; WANT %d", i, got.EndOffset, got.SyncOffset+ocfSyncLength)
		}
		headerLen = got.EndOffset // next block starts where this one ended
	}

	if total := int64(len(data)); receipts[1].EndOffset != total {
		t.Errorf("second block EndOffset: GOT %d; WANT file length %d", receipts[1].EndOffset, total)
	}

	// returned slice must be a defensive copy
	receipts[0].Complete = false
	if ocfr.BlockReceipts()[0].Complete != true {
		t.Errorf("mutating returned receipts must not affect reader state")
	}
}

// corruptSecondSync flips the first byte of the second block sync marker,
// i.e. the final 16 bytes of the file.
func corruptSecondSync(data []byte) []byte {
	out := append([]byte(nil), data...)
	out[len(out)-ocfSyncLength] ^= 0xFF
	return out
}

func TestBlockReceiptSecondSyncErrorDamagesOnlySecond(t *testing.T) {
	data := corruptSecondSync(buildTwoBlockOCF(t, CompressionNullLabel))

	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	values, err := readAllRecords(ocfr)
	if err == nil {
		t.Fatal("expected sync marker mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "sync marker mismatch") {
		t.Fatalf("error should mention sync marker mismatch: %s", err)
	}

	// all three records from the delivered first block are present and must
	// not be replayed
	if got, want := len(values), 3; got != want {
		t.Fatalf("delivered records before failure: GOT %d; WANT %d", got, want)
	}
	wantValues := []int64{13, 42, -12}
	for i := range values {
		if values[i] != wantValues[i] {
			t.Errorf("record %d: GOT %d; WANT %d", i, values[i], wantValues[i])
		}
	}

	receipts := ocfr.BlockReceipts()
	if got, want := len(receipts), 2; got != want {
		t.Fatalf("receipt count: GOT %d; WANT %d", got, want)
	}

	first := receipts[0]
	if !first.Complete || !first.SyncVerified || first.Err != nil {
		t.Errorf("first block must remain intact: Complete=%v SyncVerified=%v Err=%v",
			first.Complete, first.SyncVerified, first.Err)
	}
	if first.DecodedItems != 3 || first.DeclaredItems != 3 {
		t.Errorf("first block counts: declared=%d decoded=%d", first.DeclaredItems, first.DecodedItems)
	}

	second := receipts[1]
	if second.Stage != BlockErrorSync {
		t.Errorf("second block stage: GOT %s; WANT %s", second.Stage, BlockErrorSync)
	}
	if second.Err == nil || !strings.Contains(second.Err.Error(), "sync marker mismatch") {
		t.Errorf("second block receipt should carry sync error, GOT %v", second.Err)
	}
	if second.SyncVerified {
		t.Errorf("corrupt marker must not verify")
	}
	if second.Complete {
		t.Errorf("error block must never be marked complete")
	}
	// framing statistics up to the failed marker stay trustworthy
	if second.DeclaredItems != 2 || second.CompressedSize != 3 {
		t.Errorf("second block framing: declared=%d compressed=%d", second.DeclaredItems, second.CompressedSize)
	}
	if second.EndOffset != int64(len(data)) {
		t.Errorf("second EndOffset: GOT %d; WANT %d", second.EndOffset, len(data))
	}
	if second.DataOffset != first.EndOffset {
		t.Errorf("second DataOffset: GOT %d; WANT first EndOffset %d", second.DataOffset, first.EndOffset)
	}

	// sticky error: prior block is not replayed on further Scan calls
	if ocfr.Scan() {
		t.Errorf("Scan after fatal error must return false")
	}
	if got := len(ocfr.BlockReceipts()); got != 2 {
		t.Errorf("failed block must not be replayed, receipt count GOT %d; WANT 2", got)
	}
}

func TestBlockReceiptTruncationAttributedToTruncated(t *testing.T) {
	full := buildTwoBlockOCF(t, CompressionNullLabel)
	headerEnd := int64(bytes.Index(full, testSyncMarker[:])) + ocfSyncLength
	// first frame: 2 prefix longs + 3 payload bytes + 16 sync marker bytes
	secondBlockStart := headerEnd + 2 + 3 + ocfSyncLength

	// cut inside the second block payload: count and size present, data gone
	cut := secondBlockStart + 2
	data := full[:cut]

	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	values, err := readAllRecords(ocfr)
	if err == nil {
		t.Fatal("expected truncation error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot read block") {
		t.Fatalf("error should be a block read/truncation error: %s", err)
	}
	if got := len(values); got != 3 {
		t.Errorf("first block records delivered: GOT %d; WANT 3", got)
	}

	receipts := ocfr.BlockReceipts()
	if got := len(receipts); got != 2 {
		t.Fatalf("receipt count: GOT %d; WANT 2", got)
	}
	second := receipts[1]
	if second.Stage != BlockErrorTruncated {
		t.Errorf("stage: GOT %s; WANT %s", second.Stage, BlockErrorTruncated)
	}
	if second.Complete || second.SyncVerified {
		t.Errorf("truncated block must be incomplete")
	}
}

// buildInflatedCountBlock makes a one-block null OCF whose frame declares 4
// records while the payload only encodes 3 longs; the fraud is detected at
// the end of the block when the records run out.
func buildInflatedCountBlock(t *testing.T) []byte {
	t.Helper()
	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:          bb,
		Schema:     schemaLong,
		SyncMarker: testSyncMarker,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = ocfw.Append([]int64{13, 42, -12}); err != nil {
		t.Fatal(err)
	}
	data := bb.Bytes()
	blockStart := bytes.Index(data, testSyncMarker[:]) + ocfSyncLength
	// zigzag(3) = 0x06, zigzag(4) = 0x08: rewrite the declared block count
	data[blockStart] = 0x08
	return data
}

func TestBlockReceiptDeclaredCountLiedDetectedAtBlockEnd(t *testing.T) {
	data := buildInflatedCountBlock(t)

	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	values, readErr := readAllRecords(ocfr)

	// three records are delivered, then the next Scan/Read finds the block
	// exhausted and reports the count fraud at block end
	if got := len(values); got != 3 {
		t.Fatalf("decoded records before error: GOT %d; WANT 3", got)
	}
	if readErr == nil || !strings.Contains(readErr.Error(), "block count mismatch") {
		t.Fatalf("expected block count mismatch error, GOT: %v", readErr)
	}

	receipts := ocfr.BlockReceipts()
	if got := len(receipts); got != 1 {
		t.Fatalf("receipt count: GOT %d; WANT 1", got)
	}
	b := receipts[0]
	if b.DeclaredItems != 4 {
		t.Errorf("DeclaredItems: GOT %d; WANT 4", b.DeclaredItems)
	}
	if b.DecodedItems != 3 {
		t.Errorf("DecodedItems: GOT %d; WANT 3", b.DecodedItems)
	}
	if b.Stage != BlockErrorCount {
		t.Errorf("stage: GOT %s; WANT %s", b.Stage, BlockErrorCount)
	}
	if b.Complete {
		t.Errorf("fraudulent block must never be marked complete")
	}
}

// buildCodecErrorBlock returns a null-codec OCF whose only block payload is
// not a valid long encoding, attributing the failure to the datum codec
// rather than to truncation.
func buildCodecErrorBlock(t *testing.T) []byte {
	t.Helper()
	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:          bb,
		Schema:     schemaLong,
		SyncMarker: testSyncMarker,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = ocfw.Append([]int64{13}); err != nil {
		t.Fatal(err)
	}
	data := bb.Bytes()
	blockStart := bytes.Index(data, testSyncMarker[:]) + ocfSyncLength
	// frame: count=1 (0x02), size=1 (0x02), one payload byte. 0xFF starts a
	// long varint with its continuation bit set, but the byte stream ends.
	data[blockStart+2] = 0xFF
	return data
}

func TestBlockReceiptCodecErrorDistinctFromTruncation(t *testing.T) {
	data := buildCodecErrorBlock(t)
	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var decodeErr error
	for ocfr.Scan() {
		if _, decodeErr = ocfr.Read(); decodeErr != nil {
			break
		}
	}
	if decodeErr == nil {
		t.Fatal("expected a codec decode error")
	}
	receipts := ocfr.BlockReceipts()
	b := receipts[0]
	if b.Stage != BlockErrorCodec {
		t.Errorf("stage: GOT %s; WANT %s", b.Stage, BlockErrorCodec)
	}
	if !b.SyncVerified {
		t.Errorf("marker verified before datum decode; GOT false; WANT true")
	}
	if b.Complete {
		t.Errorf("codec-failed block must not be complete")
	}
	if b.DeclaredItems != 1 || b.DecodedItems != 0 {
		t.Errorf("counts: declared=%d decoded=%d", b.DeclaredItems, b.DecodedItems)
	}
}

func TestBlockReceiptCompressedSizeWithoutReDecompress(t *testing.T) {
	data := buildTwoBlockOCF(t, CompressionSnappyLabel)
	ocfr, err := NewOCFReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readAllRecords(ocfr); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	receipts := ocfr.BlockReceipts()
	if got := len(receipts); got != 2 {
		t.Fatalf("receipt count: GOT %d; WANT 2", got)
	}
	for i, b := range receipts {
		if b.CompressedSize <= 0 || b.DecompressedSize <= 0 {
			t.Errorf("block %d missing sizes: compressed=%d decompressed=%d", i, b.CompressedSize, b.DecompressedSize)
		}
		// these tiny incompressible blocks carry snappy framing overhead:
		// 1 length varint + 1 literal tag + 4-byte CRC32 checksum = 6 bytes
		if b.CompressedSize != b.DecompressedSize+6 {
			t.Errorf("block %d compressed=%d decompressed=%d; WANT compressed=decompressed+6",
				i, b.CompressedSize, b.DecompressedSize)
		}
		if !b.Complete {
			t.Errorf("block %d should be complete", i)
		}
	}
}
