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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

const (
	// CompressionNullLabel is used when OCF blocks are not compressed.
	CompressionNullLabel = "null"

	// CompressionDeflateLabel is used when OCF blocks are compressed using the
	// deflate algorithm.
	CompressionDeflateLabel = "deflate"

	// CompressionSnappyLabel is used when OCF blocks are compressed using the
	// snappy algorithm.
	CompressionSnappyLabel = "snappy"
)

// compressionID are values used to specify compression algorithm used to compress
// and decompress Avro Object Container File (OCF) streams.
type compressionID uint8

const (
	compressionNull compressionID = iota
	compressionDeflate
	compressionSnappy
)

const (
	ocfBlockConst      = 24 // Each OCF block has two longs prefix, and sync marker suffix
	ocfHeaderSizeConst = 48 // OCF header is usually about 48 bytes longer than its compressed schema
	ocfMagicString     = "Obj\x01"
	ocfMetadataSchema  = `{"type":"map","values":"bytes"}`
	ocfSyncLength      = 16

	// avroSchemaKey and avroCodecKey are the reserved OCF metadata keys
	// defined by the Avro specification.
	avroSchemaKey = "avro.schema"
	avroCodecKey  = "avro.codec"

	// ocfReservedKeyPrefix is the prefix of metadata keys reserved by the
	// Avro specification. Custom metadata keys must not begin with "avro.".
	ocfReservedKeyPrefix = "avro."
)

var (
	ocfMagicBytes    = []byte(ocfMagicString)
	ocfMetadataCodec *Codec
)

func init() {
	ocfMetadataCodec, _ = NewCodec(ocfMetadataSchema)
}

type ocfHeader struct {
	codec         *Codec
	compressionID compressionID
	syncMarker    [ocfSyncLength]byte
	metadata      map[string][]byte
	// metaDataSafe, when true, means metadata holds a private deep copy of
	// caller supplied data, reserved metadata keys were rejected during header
	// creation, and the header must serialize metadata keys in sorted order.
	metaDataSafe bool
}

// isReservedMetadataKey reports whether key is reserved for use by the Avro
// specification. Metadata keys beginning with "avro.", such as "avro.schema"
// and "avro.codec", are reserved.
func isReservedMetadataKey(key string) bool {
	return strings.HasPrefix(key, ocfReservedKeyPrefix)
}

// cloneMetaData returns a deep copy of m, so later mutation of the caller's
// keys or byte slice values cannot affect copied data. A nil map yields a nil
// result, preserving the distinction between "no metadata" and "empty map".
func cloneMetaData(m map[string][]byte) map[string][]byte {
	if m == nil {
		return nil
	}
	cloned := make(map[string][]byte, len(m))
	for k, v := range m {
		cloned[k] = append([]byte(nil), v...)
	}
	return cloned
}

func newOCFHeader(config OCFConfig) (*ocfHeader, error) {
	var err error

	header := new(ocfHeader)

	//
	// avro.codec
	//
	switch config.CompressionName {
	case "":
		header.compressionID = compressionNull
	case CompressionNullLabel:
		header.compressionID = compressionNull
	case CompressionDeflateLabel:
		header.compressionID = compressionDeflate
	case CompressionSnappyLabel:
		header.compressionID = compressionSnappy
	default:
		return nil, fmt.Errorf("cannot create OCF header using unrecognized compression algorithm: %q", config.CompressionName)
	}

	//
	// avro.schema
	//
	// nolint:gocritic
	if config.Codec != nil {
		header.codec = config.Codec
	} else if config.Schema == "" {
		return nil, fmt.Errorf("cannot create OCF header without either Codec or Schema specified")
	} else {
		if header.codec, err = NewCodec(config.Schema); err != nil {
			return nil, fmt.Errorf("cannot create OCF header: %s", err)
		}
	}

	if config.MetaDataSafe {
		// Validate every custom metadata key before the header is written.
		// Reserved keys ("avro.*") are always emitted by the writer itself, so
		// accepting them here would either silently overwrite the Avro schema
		// and codec values or let caller input drive them. The check happens
		// here, before writeOCFHeader and therefore before any byte is written
		// to the output stream.
		for key := range config.MetaData {
			if isReservedMetadataKey(key) {
				return nil, fmt.Errorf("cannot create OCF header with reserved metadata key: %q", key)
			}
		}
		// Defensively copy caller provided values so later mutation of the
		// caller's map or byte slices cannot change the generated header.
		header.metadata = cloneMetaData(config.MetaData)
		header.metaDataSafe = true
	} else {
		// Legacy behavior: keep the caller's map as-is, including any reserved
		// keys (which are overwritten by the canonical avro.* values below).
		header.metadata = config.MetaData
	}
	header.syncMarker = config.SyncMarker

	if header.syncMarker == [16]byte{} {
		//
		// The 16-byte, randomly-generated sync marker for this file.
		//
		_, err = rand.Read(header.syncMarker[:])
		if err != nil {
			return nil, err
		}
	}

	return header, nil
}

func readOCFHeader(ior io.Reader) (*ocfHeader, error) {
	//
	// magic bytes
	//
	magic := make([]byte, 4)
	_, err := io.ReadFull(ior, magic)
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header magic bytes: %s", err)
	}
	if !bytes.Equal(magic, ocfMagicBytes) {
		return nil, fmt.Errorf("cannot read OCF header with invalid magic bytes: %#q", magic)
	}

	//
	// metadata
	//
	metadata, err := metadataBinaryReader(ior)
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header metadata: %s", err)
	}

	//
	// avro.codec
	//
	// NOTE: Avro specification states that `null` cID is used by
	// default when "avro.codec" was not included in the metadata header. The
	// specification does not talk about the case when "avro.codec" was included
	// with the empty string as its value. I believe it is an error for an OCF
	// file to provide the empty string as the cID algorithm. While it
	// is trivially easy to gracefully handle here, I'm not sure whether this
	// happens a lot, and don't want to accept bad input unless we have
	// significant reason to do so.
	var cID compressionID
	value, ok := metadata[avroCodecKey]
	if ok {
		switch avroCodec := string(value); avroCodec {
		case CompressionNullLabel:
			cID = compressionNull
		case CompressionDeflateLabel:
			cID = compressionDeflate
		case CompressionSnappyLabel:
			cID = compressionSnappy
		default:
			return nil, fmt.Errorf("cannot read OCF header using unrecognized compression algorithm from avro.codec: %q", avroCodec)
		}
	}

	//
	// create goavro.Codec from specified avro.schema
	//
	value, ok = metadata[avroSchemaKey]
	if !ok {
		return nil, errors.New("cannot read OCF header without avro.schema")
	}
	codec, err := NewCodec(string(value))
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header with invalid avro.schema: %s", err)
	}

	header := &ocfHeader{codec: codec, compressionID: cID, metadata: metadata}

	//
	// read and store sync marker
	//
	if n, err := io.ReadFull(ior, header.syncMarker[:]); err != nil {
		return nil, fmt.Errorf("cannot read OCF header without sync marker: only read %d of %d bytes: %s", n, ocfSyncLength, err)
	}

	//
	// header is valid
	//
	return header, nil
}

func writeOCFHeader(header *ocfHeader, iow io.Writer) (err error) {
	//
	// avro.codec
	//
	var avroCodec string
	switch header.compressionID {
	case compressionNull:
		avroCodec = CompressionNullLabel
	case compressionDeflate:
		avroCodec = CompressionDeflateLabel
	case compressionSnappy:
		avroCodec = CompressionSnappyLabel
	default:
		return fmt.Errorf("should not get here: cannot write OCF header using unrecognized compression algorithm: %d", header.compressionID)
	}

	//
	// avro.schema
	//
	// Create buffer for OCF header. The first four bytes are magic, and we'll
	// use copy to fill them in, so initialize buffer's length with 4, and its
	// capacity equal to length of avro schema plus a constant.
	schema := header.codec.Schema()
	buf := make([]byte, 4, len(schema)+ocfHeaderSizeConst)
	_ = copy(buf, ocfMagicBytes)

	if header.metaDataSafe {
		// Serialize the metadata map in a single block with keys in
		// lexicographic order. This gives ordinary custom keys a stable,
		// deterministic position in the generated header regardless of Go's
		// randomized map iteration order.
		buf, err = writeOCFMetadataSorted(buf, header.metadata, schema, avroCodec)
		if err != nil {
			return fmt.Errorf("should not get here: cannot write OCF header: %s", err)
		}
	} else {
		//
		// file metadata, including the schema
		//
		meta := make(map[string]interface{})
		for k, v := range header.metadata {
			meta[k] = v
		}
		meta[avroSchemaKey] = []byte(schema)
		meta[avroCodecKey] = []byte(avroCodec)

		buf, err = ocfMetadataCodec.BinaryFromNative(buf, meta)
		if err != nil {
			return fmt.Errorf("should not get here: cannot write OCF header: %s", err)
		}
	}

	//
	// 16-byte sync marker
	//
	buf = append(buf, header.syncMarker[:]...)

	// emit OCF header
	_, err = iow.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write OCF header: %s", err)
	}
	return nil
}

// writeOCFMetadataSorted appends the OCF metadata map, encoded as an Avro
// map of bytes, to buf with every key serialized in lexicographic order. The
// reserved avro.schema and avro.codec entries are added to the custom
// metadata before sorting, so the complete header is deterministic. All
// entries are emitted in a single map block, followed by the map's zero
// block-count terminator.
func writeOCFMetadataSorted(buf []byte, custom map[string][]byte, schema, codec string) ([]byte, error) {
	keys := make([]string, 0, len(custom)+2)
	for k := range custom {
		keys = append(keys, k)
	}
	keys = append(keys, avroSchemaKey, avroCodecKey)
	sort.Strings(keys)

	var err error
	// One block containing every entry.
	buf, _ = longBinaryFromNative(buf, int64(len(keys)))
	for _, k := range keys {
		buf, _ = stringBinaryFromNative(buf, k)
		switch k {
		case avroSchemaKey:
			buf, err = bytesBinaryFromNative(buf, []byte(schema))
		case avroCodecKey:
			buf, err = bytesBinaryFromNative(buf, []byte(codec))
		default:
			buf, err = bytesBinaryFromNative(buf, custom[k])
		}
		if err != nil {
			return nil, err
		}
	}
	// Zero block count signals end of map.
	return longBinaryFromNative(buf, 0)
}
