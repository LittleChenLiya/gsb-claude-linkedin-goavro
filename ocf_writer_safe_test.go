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
	"reflect"
	"testing"
)

// headerKV is an in-memory fixture holding one decoded metadata entry in the
// exact order it appeared on the wire.
type headerKV struct {
	key   string
	value []byte
}

// decodeHeaderMetadata parses the metadata map out of a serialized OCF header
// and returns its entries in the exact on-wire order. It is a small
// in-memory test fixture used to assert key ordering.
func decodeHeaderMetadata(t *testing.T, raw []byte) []headerKV {
	t.Helper()
	buf := raw
	if len(buf) < len(ocfMagicBytes) || !bytes.Equal(buf[:len(ocfMagicBytes)], ocfMagicBytes) {
		t.Fatalf("missing OCF magic bytes: % x", buf)
	}
	buf = buf[len(ocfMagicBytes):]

	var got []headerKV
	for {
		count, rest, err := longNativeFromBinary(buf)
		if err != nil {
			t.Fatalf("cannot decode metadata block count: %s", err)
		}
		buf = rest
		blockCount := count.(int64)
		if blockCount == 0 {
			return got // map terminator
		}
		if blockCount < 0 { // negative block count is followed by a block size
			blockCount = -blockCount
			if _, buf, err = longNativeFromBinary(buf); err != nil {
				t.Fatalf("cannot decode metadata block size: %s", err)
			}
		}
		for i := int64(0); i < blockCount; i++ {
			k, rest, err := stringNativeFromBinary(buf)
			if err != nil {
				t.Fatalf("cannot decode metadata key: %s", err)
			}
			buf = rest
			v, rest, err := bytesNativeFromBinary(buf)
			if err != nil {
				t.Fatalf("cannot decode metadata value: %s", err)
			}
			buf = rest
			got = append(got, headerKV{k.(string), v.([]byte)})
		}
	}
}

// headerFromSafeWriter builds an OCF writer with MetaDataSafe enabled and
// returns the exact header bytes that were emitted before any data block.
func headerFromSafeWriter(t *testing.T, md map[string][]byte) []byte {
	t.Helper()
	bb := new(bytes.Buffer)
	_, err := NewOCFWriter(OCFConfig{
		W:            bb,
		Schema:       `{"type":"long"}`,
		MetaData:     md,
		MetaDataSafe: true,
		SyncMarker:   [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	})
	if err != nil {
		t.Fatalf("cannot create OCFWriter: %s", err)
	}
	return append([]byte(nil), bb.Bytes()...)
}

// TestOCFSafeMetaDataDefensiveCopy verifies the core copy-on-write contract:
// after writing two custom items, mutating either the caller's original byte
// slices or the byte slices returned by the writer leaves the already written
// header unchanged.
func TestOCFSafeMetaDataDefensiveCopy(t *testing.T) {
	// Two custom items owned by the caller.
	alpha := []byte("alpha-value")
	beta := []byte("beta-value")
	custom := map[string][]byte{"alpha": alpha, "beta": beta}

	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:            bb,
		Schema:       `{"type":"long"}`,
		MetaData:     custom,
		MetaDataSafe: true,
		SyncMarker:   [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	})
	if err != nil {
		t.Fatalf("cannot create OCFWriter: %s", err)
	}
	headerAfterWrite := append([]byte(nil), bb.Bytes()...)

	// Mutate the slices the caller passed in, both via the local slices and
	// via the values reachable through the caller's map.
	for i := range alpha {
		alpha[i] = 'X'
	}
	for i := range beta {
		beta[i] = 'Y'
	}
	custom["alpha"][0] = 'Z'

	// Mutate the slices returned by the writer as well.
	returned := ocfw.MetaData()
	for k := range returned {
		for i := range returned[k] {
			returned[k][i] = '!'
		}
	}
	returned["alpha"] = append(returned["alpha"], []byte("-tampered")...)

	// Header bytes written before the mutations must be unchanged.
	if !bytes.Equal(bb.Bytes(), headerAfterWrite) {
		t.Fatalf("header changed after mutating caller and returned slices\nwant: % x\n got: % x", headerAfterWrite, bb.Bytes())
	}

	// Reading the file back must still surface the original values.
	ocfr, err := NewOCFReader(bytes.NewReader(append([]byte(nil), bb.Bytes()...)))
	if err != nil {
		t.Fatalf("cannot create OCFReader: %s", err)
	}
	readMeta := ocfr.MetaData()
	if got, want := string(readMeta["alpha"]), "alpha-value"; got != want {
		t.Errorf("alpha: GOT %q; WANT %q", got, want)
	}
	if got, want := string(readMeta["beta"]), "beta-value"; got != want {
		t.Errorf("beta: GOT %q; WANT %q", got, want)
	}
	// A fresh getter result is also unaffected.
	fresh := ocfw.MetaData()
	if got, want := string(fresh["alpha"]), "alpha-value"; got != want {
		t.Errorf("fresh alpha: GOT %q; WANT %q", got, want)
	}
}

// TestOCFSafeMetaDataSetEntryPoint exercises the safe modification entry point
// on OCFConfig, including deep copy of its input.
func TestOCFSafeMetaDataSetEntryPoint(t *testing.T) {
	v := []byte("entry-value")
	var config OCFConfig
	if err := config.SetMetaData(map[string][]byte{"entry": v}); err != nil {
		t.Fatalf("SetMetaData returned error: %s", err)
	}
	if !config.MetaDataSafe {
		t.Fatal("SetMetaData must enable MetaDataSafe")
	}
	// Mutate the input after handing it to SetMetaData.
	for i := range v {
		v[i] = '?'
	}
	if got, want := string(config.MetaData["entry"]), "entry-value"; got != want {
		t.Errorf("GOT %q; WANT %q", got, want)
	}

	// Reserved keys are rejected and nothing is stored.
	for _, reserved := range []string{"avro.schema", "avro.codec", "avro.anything"} {
		var cfg OCFConfig
		err := cfg.SetMetaData(map[string][]byte{reserved: []byte("nope")})
		if err == nil {
			t.Errorf("SetMetaData(%q): expected error, got nil", reserved)
		}
		if cfg.MetaData != nil {
			t.Errorf("SetMetaData(%q): expected no stored metadata, got %v", reserved, cfg.MetaData)
		}
	}
}

// TestOCFSafeMetaDataReservedKeyRejected verifies that attempting to override
// reserved keys fails before the header is written, so the output receives
// zero bytes.
func TestOCFSafeMetaDataReservedKeyRejected(t *testing.T) {
	for _, reserved := range []string{"avro.schema", "avro.codec"} {
		t.Run(reserved, func(t *testing.T) {
			bb := new(bytes.Buffer)
			_, err := NewOCFWriter(OCFConfig{
				W:            bb,
				Schema:       `{"type":"long"}`,
				MetaData:     map[string][]byte{reserved: []byte("must-not-win")},
				MetaDataSafe: true,
			})
			if err == nil {
				t.Fatal("expected error for reserved metadata key, got nil")
			}
			if bb.Len() != 0 {
				t.Fatalf("expected zero output before header write, got %d bytes: % x", bb.Len(), bb.Bytes())
			}
		})
	}
}

// TestOCFSafeMetaDataStableOrder verifies that ordinary custom keys are
// serialized in stable lexicographic order, independent of map insertion
// order, and that output is reproducible across runs.
func TestOCFSafeMetaDataStableOrder(t *testing.T) {
	entries := []struct {
		key string
		val string
	}{
		{"zulu", "1"},
		{"mike", "2"},
		{"alpha", "3"},
	}

	var reference []byte
	for iter := 0; iter < 8; iter++ {
		custom := make(map[string][]byte, len(entries))
		// Rotate insertion order each iteration.
		order := []int{0, 1, 2}
		for shift := 0; shift < iter%3; shift++ {
			order = append(order[1:], order[0])
		}
		for _, idx := range order {
			custom[entries[idx].key] = []byte(entries[idx].val)
		}

		header := headerFromSafeWriter(t, custom)

		if reference == nil {
			reference = header
		} else if !bytes.Equal(header, reference) {
			t.Fatalf("iteration %d produced a different header\nwant: % x\n got: % x", iter, reference, header)
		}

		kv := decodeHeaderMetadata(t, header)
		keys := make([]string, 0, len(kv))
		for _, e := range kv {
			keys = append(keys, e.key)
		}
		wantKeys := []string{"alpha", avroCodecKey, avroSchemaKey, "mike", "zulu"}
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("iteration %d key order: GOT %v; WANT %v", iter, keys, wantKeys)
		}
	}
}

// TestOCFLegacyMetaDataUnchanged documents the opt-in contract: when
// MetaDataSafe is not enabled the original interface keeps working, reserved
// keys are accepted and overwritten, and ordinary custom entries round trip.
func TestOCFLegacyMetaDataUnchanged(t *testing.T) {
	bb := new(bytes.Buffer)
	// Legacy behavior tolerates a caller supplied reserved key and overrides it.
	custom := map[string][]byte{
		"foo":         []byte("BOING"),
		"avro.schema": []byte(`{"type":"int"}`), // must be overridden
	}
	ocfw, err := NewOCFWriter(OCFConfig{
		W:          bb,
		Schema:     `{"type":"long"}`,
		MetaData:   custom,
		SyncMarker: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	})
	if err != nil {
		t.Fatalf("legacy NewOCFWriter returned error: %s", err)
	}
	if err := ocfw.Append([]int64{13, 42}); err != nil {
		t.Fatalf("Append: %s", err)
	}

	ocfr, err := NewOCFReader(bytes.NewReader(append([]byte(nil), bb.Bytes()...)))
	if err != nil {
		t.Fatalf("NewOCFReader: %s", err)
	}
	meta := ocfr.MetaData()
	if got, want := string(meta["foo"]), "BOING"; got != want {
		t.Errorf("foo: GOT %q; WANT %q", got, want)
	}
	if got, want := string(meta[avroSchemaKey]), `{"type":"long"}`; got != want {
		t.Errorf("schema must be writer controlled: GOT %q; WANT %q", got, want)
	}
	for ocfr.Scan() {
		if _, err := ocfr.Read(); err != nil {
			t.Fatalf("Read: %s", err)
		}
	}
	if err := ocfr.Err(); err != nil {
		t.Fatalf("reader error: %s", err)
	}

	// Writer getter returns only custom keys and a usable copy.
	wm := ocfw.MetaData()
	if _, ok := wm[avroSchemaKey]; ok {
		t.Errorf("writer MetaData must not expose reserved %q", avroSchemaKey)
	}
	if got, want := string(wm["foo"]), "BOING"; got != want {
		t.Errorf("writer foo: GOT %q; WANT %q", got, want)
	}
}
