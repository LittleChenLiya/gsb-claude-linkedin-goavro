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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOCFWriterMetaDataEntriesImmutability ensures that mutating the byte
// slices handed in by the caller, or the byte slices returned by
// OCFWriter.MetaData, cannot modify the header that has been generated.
func TestOCFWriterMetaDataEntriesImmutability(t *testing.T) {
	value1 := []byte("first-value")
	value2 := []byte("second-value")

	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:      bb,
		Schema: `{"type":"long"}`,
		MetaDataEntries: []OCFMetaDataEntry{
			{Key: "zeta", Value: value1},
			{Key: "alpha", Value: value2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot the header bytes that were written out.
	headerSnapshot := append([]byte(nil), bb.Bytes()...)

	// Mutate the caller-provided slices after the writer has been created.
	for i := range value1 {
		value1[i] = 'X'
	}
	for i := range value2 {
		value2[i] = 'X'
	}

	// Mutate the slices returned by the MetaData getter.
	returned := ocfw.MetaData()
	for k, v := range returned {
		for i := range v {
			v[i] = 'Y'
		}
		returned[k] = []byte("garbage")
	}

	// The writer's metadata must be unaffected by either mutation.
	actual := ocfw.MetaData()
	if got, want := string(actual["zeta"]), "first-value"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
	if got, want := string(actual["alpha"]), "second-value"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}

	// The header bytes already written out must be unchanged.
	if !bytes.Equal(bb.Bytes(), headerSnapshot) {
		t.Errorf("GOT: %v; WANT: %v", bb.Bytes(), headerSnapshot)
	}

	// Reading the OCF back must show the original metadata values.
	ocfr, err := NewOCFReader(bytes.NewReader(bb.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	readMeta := ocfr.MetaData()
	if got, want := string(readMeta["zeta"]), "first-value"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
	if got, want := string(readMeta["alpha"]), "second-value"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
}

// TestOCFWriterMetaDataEntriesReservedKeys ensures reserved "avro." keys are
// rejected with an error before anything is written to the output.
func TestOCFWriterMetaDataEntriesReservedKeys(t *testing.T) {
	for _, key := range []string{"avro.schema", "avro.codec", "avro.anything"} {
		bb := new(bytes.Buffer)
		_, err := NewOCFWriter(OCFConfig{
			W:      bb,
			Schema: `{"type":"long"}`,
			MetaDataEntries: []OCFMetaDataEntry{
				{Key: key, Value: []byte("nope")},
			},
		})
		if err == nil {
			t.Errorf("GOT: no error; WANT: error for reserved key %q", key)
		} else if !strings.Contains(err.Error(), key) {
			t.Errorf("GOT: %v; WANT: error mentioning %q", err, key)
		}
		if actual := bb.Len(); actual != 0 {
			t.Errorf("GOT: %d bytes written; WANT: 0 bytes written for reserved key %q", actual, key)
		}
	}
}

// TestOCFWriterMetaDataEntriesStableOrder ensures custom metadata keys are
// written in a stable (sorted) order regardless of the order in which the
// entries were specified.
func TestOCFWriterMetaDataEntriesStableOrder(t *testing.T) {
	syncMarker := [16]byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}

	newHeader := func(entries []OCFMetaDataEntry) []byte {
		bb := new(bytes.Buffer)
		_, err := NewOCFWriter(OCFConfig{
			W:               bb,
			Schema:          `{"type":"long"}`,
			SyncMarker:      syncMarker,
			MetaDataEntries: entries,
		})
		if err != nil {
			t.Fatal(err)
		}
		return bb.Bytes()
	}

	forward := newHeader([]OCFMetaDataEntry{
		{Key: "alpha", Value: []byte("one")},
		{Key: "zeta", Value: []byte("two")},
	})
	reverse := newHeader([]OCFMetaDataEntry{
		{Key: "zeta", Value: []byte("two")},
		{Key: "alpha", Value: []byte("one")},
	})

	if !bytes.Equal(forward, reverse) {
		t.Errorf("GOT: %v; WANT: %v", forward, reverse)
	}

	// Custom keys appear in sorted order within the header bytes.
	alphaIndex := bytes.Index(forward, []byte("alpha"))
	zetaIndex := bytes.Index(forward, []byte("zeta"))
	if alphaIndex < 0 || zetaIndex < 0 {
		t.Fatalf("GOT: alpha=%d zeta=%d; WANT: both keys present in header", alphaIndex, zetaIndex)
	}
	if alphaIndex > zetaIndex {
		t.Errorf("GOT: alpha@%d after zeta@%d; WANT: alpha before zeta", alphaIndex, zetaIndex)
	}
}

// TestOCFWriterMetaDataEntriesOverrideMap ensures an entry overrides a
// MetaData map item having the same key, and that the original MetaData map
// interface keeps working when the new option is not used.
func TestOCFWriterMetaDataEntriesOverrideMap(t *testing.T) {
	bb := new(bytes.Buffer)
	_, err := NewOCFWriter(OCFConfig{
		W:        bb,
		Schema:   `{"type":"long"}`,
		MetaData: map[string][]byte{"foo": []byte("from-map"), "goo": []byte("zoo")},
		MetaDataEntries: []OCFMetaDataEntry{
			{Key: "foo", Value: []byte("from-entry")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ocfr, err := NewOCFReader(bytes.NewReader(bb.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	readMeta := ocfr.MetaData()
	if got, want := string(readMeta["foo"]), "from-entry"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
	if got, want := string(readMeta["goo"]), "zoo"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
}

// TestOCFWriterMetaDataGetterOnAppend ensures the MetaData getter also works
// when appending to an existing OCF file, returning an independent copy of
// the metadata read from the existing header.
func TestOCFWriterMetaDataGetterOnAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.avro")

	fh, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewOCFWriter(OCFConfig{
		W:      fh,
		Schema: `{"type":"long"}`,
		MetaDataEntries: []OCFMetaDataEntry{
			{Key: "foo", Value: []byte("bar")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-open the existing OCF for appending; the header, including its
	// metadata, is read back from the file.
	fh, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	ocfw, err := NewOCFWriter(OCFConfig{W: fh})
	if err != nil {
		t.Fatal(err)
	}

	m1 := ocfw.MetaData()
	if got, want := string(m1["foo"]), "bar"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
	// Mutating the returned map and its slices must not affect the writer.
	m1["injected"] = []byte("nope")
	for i := range m1["foo"] {
		m1["foo"][i] = 'X'
	}
	m2 := ocfw.MetaData()
	if _, ok := m2["injected"]; ok {
		t.Errorf("GOT: injected key present; WANT: getter returns independent copy")
	}
	if got, want := string(m2["foo"]), "bar"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
}
