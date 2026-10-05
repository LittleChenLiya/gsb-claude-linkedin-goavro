// Command blockreceipt demonstrates explicit OCF block receipts on a tiny,
// two-block Avro Object Container File built entirely in memory. It prints the
// record count, compressed/decompressed sizes, sync marker verification and
// original-file offsets of every block, then repeats the run after corrupting
// the second block sync marker to show that only the second block is damaged.
//
// Run with:
//
//	go run ./examples/blockreceipt
package main

import (
	"bytes"
	"fmt"
	"os"

	goavro "github.com/linkedin/goavro/v2"
)

var syncMarker = [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func buildTwoBlockFile() []byte {
	bb := new(bytes.Buffer)
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               bb,
		Schema:          "{\"type\":\"long\"}",
		CompressionName: goavro.CompressionSnappyLabel,
		SyncMarker:      syncMarker,
	})
	if err != nil {
		fail(err)
	}
	if err = ocfw.Append([]int64{13, 42, -12}); err != nil { // block 0: 3 records
		fail(err)
	}
	if err = ocfw.Append([]int64{-1234, 7}); err != nil { // block 1: 2 records
		fail(err)
	}
	return bb.Bytes()
}

func printReceipts(label string, data []byte) {
	fmt.Printf("== %s ==\n", label)
	ocfr, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		fail(err)
	}
	records := 0
	for ocfr.Scan() {
		if _, err := ocfr.Read(); err != nil {
			break
		}
		records++
	}
	if err := ocfr.Err(); err != nil {
		fmt.Printf("read stopped after %d delivered record(s): %s\n", records, err)
	} else {
		fmt.Printf("read all %d records cleanly\n", records)
	}
	for _, b := range ocfr.BlockReceipts() {
		status := "COMPLETE"
		if !b.Complete {
			status = "INCOMPLETE"
		}
		fmt.Printf("block %d [%s] stage=%-9s offset=%d payload=%d sync=%d end=%d "+
			"declared=%d decoded=%d compressed=%dB decompressed=%dB syncOK=%t err=%v\n",
			b.Index, status, b.Stage,
			b.DataOffset, b.PayloadOffset, b.SyncOffset, b.EndOffset,
			b.DeclaredItems, b.DecodedItems,
			b.CompressedSize, b.DecompressedSize, b.SyncVerified, b.Err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	good := buildTwoBlockFile()
	printReceipts("healthy two-block file", good)

	// Corrupt only the final 16-byte marker: the second block sync marker.
	bad := append([]byte(nil), good...)
	bad[len(bad)-16] ^= 0xFF
	printReceipts("second sync marker corrupted", bad)
}
