// Copyright [2026] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

// Acceptance tests for explicit union branch identity (NewUnionBranch /
// UnionBranch / BranchFromNative).
//
// Run with:
//
//	go test -run 'TestUnionExplicitBranch|TestUnionDefaultFirstBranch|TestUnionNoPartialBinary' -v .
//
// or simply:
//
//	go test -run Branch -v .

import (
	"bytes"
	"fmt"
	"testing"
)

// The three schemas used below:
//
//	two records with identical field structure but distinct names ...
const unionBranchRecordsSchema = `{
  "type": "record",
  "name": "Envelope",
  "fields": [
    {
      "name": "pet",
      "type": [
        "null",
        {"type": "record", "name": "Dog", "fields": [
          {"name": "name", "type": "string"}
        ]},
        {"type": "record", "name": "Cat", "fields": [
          {"name": "name", "type": "string"}
        ]}
      ]
    }
  ]
}`

// ... and a union of anonymous primitives whose Go values would otherwise
// collide.
const unionBranchPrimitivesSchema = `["null","int","long","string"]`

func dogOrCatName(name string) map[string]interface{} {
	return map[string]interface{}{"name": name}
}

// TestUnionExplicitBranchTwoRecords proves that two records with identical
// structure but different names are distinguished solely by the explicit
// branch identity, never by inspecting the Go value.
func TestUnionExplicitBranchTwoRecords(t *testing.T) {
	codec, err := NewCodec(unionBranchRecordsSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Explicit branch selects Dog (union index 1): 0x02, then record string
	// length 6 zig-zag => 0x0c, then "lassie".
	dog := map[string]interface{}{
		"pet": NewUnionBranch("Dog", dogOrCatName("lassie")),
	}
	dogBinary, err := codec.BinaryFromNative(nil, dog)
	if err != nil {
		t.Fatalf("Dog branch encode: %v", err)
	}
	if want := []byte{0x02, 0x0c, 'l', 'a', 's', 's', 'i', 'e'}; !bytes.Equal(dogBinary, want) {
		t.Fatalf("GOT: %#v; WANT: %#v", dogBinary, want)
	}

	// Same Go value shape, explicit branch selects Cat (union index 2):
	// 0x04, then length 3 zig-zag 0x06, then "rex".
	cat := map[string]interface{}{
		"pet": NewUnionBranch("Cat", dogOrCatName("rex")),
	}
	catBinary, err := codec.BinaryFromNative(nil, cat)
	if err != nil {
		t.Fatalf("Cat branch encode: %v", err)
	}
	if want := []byte{0x04, 0x06, 'r', 'e', 'x'}; !bytes.Equal(catBinary, want) {
		t.Fatalf("GOT: %#v; WANT: %#v", catBinary, want)
	}

	if bytes.Equal(dogBinary, catBinary) {
		t.Fatalf("identical structure encoded identically regardless of branch: %#v", dogBinary)
	}

	// Decoded branch identity is recoverable without reaching into the map.
	for _, tc := range []struct {
		binary []byte
		branch string
		value  map[string]interface{}
	}{
		{dogBinary, "Dog", dogOrCatName("lassie")},
		{catBinary, "Cat", dogOrCatName("rex")},
	} {
		native, rest, err := codec.NativeFromBinary(tc.binary)
		if err != nil {
			t.Fatalf("decode %q: %v", tc.branch, err)
		}
		if len(rest) != 0 {
			t.Fatalf("%q trailing bytes: %d", tc.branch, len(rest))
		}
		envelope, ok := native.(map[string]interface{})
		if !ok {
			t.Fatalf("%q envelope: %T", tc.branch, native)
		}
		choice, err := BranchFromNative(envelope["pet"])
		if err != nil {
			t.Fatalf("%q branch identity: %v", tc.branch, err)
		}
		if choice.Branch != tc.branch {
			t.Errorf("GOT branch: %q; WANT: %q", choice.Branch, tc.branch)
		}
		gotMap, ok := choice.Datum.(map[string]interface{})
		if !ok {
			t.Fatalf("%q datum type: %T", tc.branch, choice.Datum)
		}
		if gotMap["name"] != tc.value["name"] {
			t.Errorf("%q name: GOT %v; WANT %v", tc.branch, gotMap["name"], tc.value["name"])
		}
	}
}

// TestUnionExplicitBranchUnknownBranch proves selecting a branch that does
// not belong to the writer schema fails, rather than silently encoding the
// value as another structurally compatible branch.
func TestUnionExplicitBranchUnknownBranch(t *testing.T) {
	codec, err := NewCodec(unionBranchRecordsSchema)
	if err != nil {
		t.Fatal(err)
	}

	// "Bird" is not a member of the union, even though its datum has the
	// exact same shape as Dog/Cat.
	datum := map[string]interface{}{
		"pet": NewUnionBranch("Bird", dogOrCatName("tweety")),
	}
	if buf, err := codec.BinaryFromNative(nil, datum); err == nil {
		t.Fatalf("expected error for non-member branch; encoded: %#v", buf)
	}

	// Same rule at the bare union codec, including a primitive name that is
	// not present.
	primitiveCodec, err := NewCodec(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := primitiveCodec.BinaryFromNative(nil, NewUnionBranch("double", 3.0)); err == nil {
		t.Fatal("expected error for non-member branch \"double\"")
	}
	// The legacy dynamic wrapper is likewise constrained to writer members.
	if _, err := primitiveCodec.BinaryFromNative(nil, Union("double", 3.0)); err == nil {
		t.Fatal("expected error for dynamic wrapper with non-member branch")
	}
}

// TestUnionExplicitBranchNullRules proves null is its own explicit branch
// and that a nil datum pinned to a non-null branch is rejected.
func TestUnionExplicitBranchNullRules(t *testing.T) {
	codec, err := NewCodec(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Bare nil and explicit null branch both select the null member.
	for _, datum := range []interface{}{
		nil,
		NewUnionBranch("null", nil),
	} {
		buf, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			t.Fatalf("null branch: %v", err)
		}
		if want := []byte{0x00}; !bytes.Equal(buf, want) {
			t.Fatalf("GOT: %#v; WANT: %#v", buf, want)
		}
	}

	// A nil value cannot masquerade as a non-null branch.
	if _, err := codec.BinaryFromNative(nil, NewUnionBranch("int", nil)); err == nil {
		t.Fatal("expected error for nil datum on non-null branch")
	}
	// A non-nil value cannot masquerade as the null branch.
	if _, err := codec.BinaryFromNative(nil, NewUnionBranch("null", 3)); err == nil {
		t.Fatal("expected error for non-nil datum on null branch")
	}
}

// TestUnionExplicitBranchAnonymousPrimitives proves anonymous primitives
// are taken at face value from the explicit branch identity rather than
// guessed from the Go value type.
func TestUnionExplicitBranchAnonymousPrimitives(t *testing.T) {
	codec, err := NewCodec(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Go int 3 encodes as int (index 1) or long (index 2) depending only on
	// the explicit branch; the wire values differ in the index byte.
	intBinary, err := codec.BinaryFromNative(nil, NewUnionBranch("int", 3))
	if err != nil {
		t.Fatalf("int branch: %v", err)
	}
	if want := []byte{0x02, 0x06}; !bytes.Equal(intBinary, want) {
		t.Fatalf("int GOT: %#v; WANT: %#v", intBinary, want)
	}
	longBinary, err := codec.BinaryFromNative(nil, NewUnionBranch("long", 3))
	if err != nil {
		t.Fatalf("long branch: %v", err)
	}
	if want := []byte{0x04, 0x06}; !bytes.Equal(longBinary, want) {
		t.Fatalf("long GOT: %#v; WANT: %#v", longBinary, want)
	}

	// Round trip preserves explicit branch identity.
	native, rest, err := codec.NativeFromBinary(longBinary)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("trailing bytes: %d", len(rest))
	}
	choice, err := BranchFromNative(native)
	if err != nil {
		t.Fatal(err)
	}
	if choice.Branch != "long" {
		t.Fatalf("GOT branch: %q; WANT long", choice.Branch)
	}
}

// TestUnionExplicitBranchTextual proves the explicit branch identity works
// for textual (Avro JSON) encoding too.
func TestUnionExplicitBranchTextual(t *testing.T) {
	codec, err := NewCodec(unionBranchRecordsSchema)
	if err != nil {
		t.Fatal(err)
	}

	dog := map[string]interface{}{
		"pet": NewUnionBranch("Dog", dogOrCatName("lassie")),
	}
	text, err := codec.TextualFromNative(nil, dog)
	if err != nil {
		t.Fatalf("textual Dog: %v", err)
	}
	if want := []byte(`{"pet":{"Dog":{"name":"lassie"}}}`); !bytes.Equal(text, want) {
		t.Fatalf("GOT: %s; WANT: %s", text, want)
	}

	native, rest, err := codec.NativeFromTextual(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("trailing bytes: %d", len(rest))
	}
	envelope := native.(map[string]interface{})
	choice, err := BranchFromNative(envelope["pet"])
	if err != nil {
		t.Fatal(err)
	}
	if choice.Branch != "Dog" {
		t.Fatalf("GOT: %q; WANT Dog", choice.Branch)
	}
}

// TestUnionExplicitBranchDynamicInterfacePreserved proves the ordinary
// dynamic Union wrapper continues to behave exactly as before.
func TestUnionExplicitBranchDynamicInterfacePreserved(t *testing.T) {
	codec, err := NewCodec(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Dynamic nil => null member.
	buf, err := codec.BinaryFromNative(nil, Union("null", nil))
	if err != nil {
		t.Fatalf("dynamic null: %v", err)
	}
	if want := []byte{0x00}; !bytes.Equal(buf, want) {
		t.Fatalf("dynamic null GOT: %#v; WANT: %#v", buf, want)
	}
	// Dynamic explicit branch.
	buf, err = codec.BinaryFromNative(nil, Union("string", "x"))
	if err != nil {
		t.Fatalf("dynamic string: %v", err)
	}
	if want := []byte{0x06, 0x02, 'x'}; !bytes.Equal(buf, want) {
		t.Fatalf("dynamic string GOT: %#v; WANT: %#v", buf, want)
	}
	// Bare non-map values are still rejected.
	if _, err := codec.BinaryFromNative(nil, 3); err == nil {
		t.Fatal("expected error for bare non-nil value")
	}
}

// TestUnionDefaultFirstBranch proves union field defaults are validated
// against the real rules of the first branch (the Avro default resolution
// rule), and that a null value is distinct from an omitted field.
func TestUnionDefaultFirstBranch(t *testing.T) {
	// First branch null: a null default validates; a non-null default does
	// not encode under the first branch and is rejected at codec build time.
	_, err := NewCodec(`{
	  "type": "record",
	  "name": "NullFirst",
	  "fields": [
	    {"name": "u", "type": ["null", "int"], "default": null}
	  ]
	}`)
	if err != nil {
		t.Fatalf("null default on null-first union ought to validate: %v", err)
	}
	_, err = NewCodec(`{
	  "type": "record",
	  "name": "NullFirstBad",
	  "fields": [
	    {"name": "u", "type": ["null", "int"], "default": 7}
	  ]
	}`)
	if err == nil {
		t.Fatal("expected codec build error for non-null default on null-first union")
	}

	// First branch int: an int default validates; a null default does not
	// encode as int and must be rejected (defaults follow the first member,
	// not whichever branch happens to accept null).
	_, err = NewCodec(`{
	  "type": "record",
	  "name": "IntFirst",
	  "fields": [
	    {"name": "u", "type": ["int", "null"], "default": 7}
	  ]
	}`)
	if err != nil {
		t.Fatalf("int default on int-first union ought to validate: %v", err)
	}
	_, err = NewCodec(`{
	  "type": "record",
	  "name": "IntFirstBad",
	  "fields": [
	    {"name": "u", "type": ["int", "null"], "default": null}
	  ]
	}`)
	if err == nil {
		t.Fatal("expected codec build error for null default on int-first union")
	}

	// With a validated null default, an omitted field resolves to the
	// default (union index 0 -> 0x00).
	nullDefaultCodec, err := NewCodec(`{
	  "type": "record",
	  "name": "NullDefault",
	  "fields": [
	    {"name": "u", "type": ["null", "int"], "default": null}
	  ]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	omitted, err := nullDefaultCodec.BinaryFromNative(nil, map[string]interface{}{})
	if err != nil {
		t.Fatalf("omitted field: %v", err)
	}
	if want := []byte{0x00}; !bytes.Equal(omitted, want) {
		t.Fatalf("omitted (default null) GOT: %#v; WANT: %#v", omitted, want)
	}
	explicitNull, err := nullDefaultCodec.BinaryFromNative(nil, map[string]interface{}{
		"u": NewUnionBranch("null", nil),
	})
	if err != nil {
		t.Fatalf("explicit null: %v", err)
	}
	if !bytes.Equal(explicitNull, omitted) {
		t.Fatalf("explicit null %#v ought to match null default %#v", explicitNull, omitted)
	}

	// Int-first union with int default: an omitted field uses the DEFAULT
	// (int branch, 7), while an explicit null value selects the NULL branch;
	// the two wire encodings differ.
	intDefaultCodec, err := NewCodec(`{
	  "type": "record",
	  "name": "IntDefault",
	  "fields": [
	    {"name": "u", "type": ["int", "null"], "default": 7}
	  ]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	omittedUsesIntDefault, err := intDefaultCodec.BinaryFromNative(nil, map[string]interface{}{})
	if err != nil {
		t.Fatalf("omitted field: %v", err)
	}
	if want := []byte{0x00, 0x0e}; !bytes.Equal(omittedUsesIntDefault, want) { // index 0 int, 7 zig-zag => 0x0e
		t.Fatalf("omitted GOT: %#v; WANT: %#v", omittedUsesIntDefault, want)
	}
	explicitNullBranch, err := intDefaultCodec.BinaryFromNative(nil, map[string]interface{}{
		"u": NewUnionBranch("null", nil),
	})
	if err != nil {
		t.Fatalf("explicit null branch: %v", err)
	}
	if want := []byte{0x02}; !bytes.Equal(explicitNullBranch, want) { // index 1 null
		t.Fatalf("explicit null GOT: %#v; WANT: %#v", explicitNullBranch, want)
	}
	if bytes.Equal(omittedUsesIntDefault, explicitNullBranch) {
		t.Fatal("null value and missing-with-default must not encode identically")
	}
}

// TestUnionNoPartialBinaryOnFailure proves a failure inside a member datum
// does not publish the already-written union branch index: when encoding
// fails, no bytes are appended to the caller-supplied buffer.
func TestUnionNoPartialBinaryOnFailure(t *testing.T) {
	codec, err := NewCodec(`["null","int"]`)
	if err != nil {
		t.Fatal(err)
	}

	// A non-numeric datum on the int branch fails member validation. The
	// returned buffer must be exactly the original prefix, never the prefix
	// plus a partially written branch index.
	prefix := []byte{0xaa, 0xbb}
	got, err := codec.BinaryFromNative(prefix, NewUnionBranch("int", "not-a-number"))
	if err == nil {
		t.Fatal("expected member encoding error")
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("failure published partial binary data: GOT %#v; WANT original %#v", got, prefix)
	}
	// A success on the same schema proves successful encoding does append,
	// so the assertion above is meaningful.
	okBuf, err := codec.BinaryFromNative(prefix, NewUnionBranch("int", 3))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0xaa, 0xbb, 0x02, 0x06}; !bytes.Equal(okBuf, want) {
		t.Fatalf("success path GOT: %#v; WANT: %#v", okBuf, want)
	}

	// Directly exercise the union encoder with a backing buffer that has
	// spare capacity; the branch index must not be appended on failure.
	backing := make([]byte, 2, 16)
	copy(backing, prefix)
	direct, err := codec.binaryFromNative(backing, NewUnionBranch("int", "not-a-number"))
	if err == nil {
		t.Fatal("expected internal member encoding error")
	}
	if direct != nil {
		t.Fatalf("internal encoder returned partial bytes: %#v", direct)
	}
	if len(backing) != 2 || backing[0] != 0xaa || backing[1] != 0xbb {
		t.Fatalf("caller buffer mutated on failure: %#v", backing)
	}

	// The failure also propagates through a containing record without the
	// record prefix or union index leaking.
	recordCodec, err := NewCodec(`{
	  "type": "record",
	  "name": "Outer",
	  "fields": [
	    {"name": "a", "type": "string"},
	    {"name": "u", "type": ["null", "int"]}
	  ]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	recordPrefix := []byte{0xde, 0xad}
	recordGot, err := recordCodec.BinaryFromNative(recordPrefix, map[string]interface{}{
		"a": "hello",
		"u": NewUnionBranch("int", "not-a-number"),
	})
	if err == nil {
		t.Fatal("expected record encoding error")
	}
	if !bytes.Equal(recordGot, recordPrefix) {
		t.Fatalf("record failure leaked bytes: GOT %#v; WANT %#v", recordGot, recordPrefix)
	}

	// Textual encoding is likewise atomic: a member failure must not leave a
	// partially written `{"branch":` wrapper on the caller's buffer.
	textPrefix := []byte("PREFIX-")
	textGot, err := codec.TextualFromNative(textPrefix, NewUnionBranch("int", "not-a-number"))
	if err == nil {
		t.Fatal("expected textual encoding error")
	}
	if !bytes.Equal(textGot, textPrefix) {
		t.Fatalf("textual failure leaked bytes: GOT %q; WANT %q", textGot, textPrefix)
	}
}

// TestUnionExplicitBranchStandardJSON proves the dynamic standard-JSON
// codecs also honor an explicit branch identity while retaining their
// ordinary behavior.
func TestUnionExplicitBranchStandardJSON(t *testing.T) {
	full, err := NewCodecForStandardJSONFull(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}
	// Full two-way codec emits plain JSON without the Avro wrapper.
	text, err := full.TextualFromNative(nil, NewUnionBranch("string", "x"))
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	if want := []byte(`"x"`); !bytes.Equal(text, want) {
		t.Fatalf("full GOT: %s; WANT: %s", text, want)
	}

	oneWay, err := NewCodecForStandardJSON(unionBranchPrimitivesSchema)
	if err != nil {
		t.Fatal(err)
	}
	text, err = oneWay.TextualFromNative(nil, NewUnionBranch("string", "x"))
	if err != nil {
		t.Fatalf("one-way: %v", err)
	}
	if want := []byte(`{"string":"x"}`); !bytes.Equal(text, want) {
		t.Fatalf("one-way GOT: %s; WANT: %s", text, want)
	}

	// Invalid explicit branch still fails.
	if _, err := full.TextualFromNative(nil, NewUnionBranch("double", 1.0)); err == nil {
		t.Fatal("expected error for non-member branch in standard JSON")
	}
}

// ExampleNewUnionBranch shows the three-schema scenario: the same native Go
// shape can belong to two different named records, and null is its own
// branch. The branch is explicit rather than guessed from the Go value.
func ExampleNewUnionBranch() {
	codec, err := NewCodec(unionBranchRecordsSchema)
	if err != nil {
		panic(err)
	}

	for _, choice := range []UnionBranch{
		NewUnionBranch("Dog", map[string]interface{}{"name": "lassie"}),
		NewUnionBranch("Cat", map[string]interface{}{"name": "rex"}),
		NewUnionBranch("null", nil),
	} {
		binary, err := codec.BinaryFromNative(nil, map[string]interface{}{"pet": choice})
		if err != nil {
			panic(err)
		}
		native, _, err := codec.NativeFromBinary(binary)
		if err != nil {
			panic(err)
		}
		decoded, err := BranchFromNative(native.(map[string]interface{})["pet"])
		if err != nil {
			panic(err)
		}
		if decoded.Datum == nil {
			fmt.Printf("%s: null\n", decoded.Branch)
		} else {
			fmt.Printf("%s: %v\n", decoded.Branch, decoded.Datum.(map[string]interface{})["name"])
		}
	}
	// Output:
	// Dog: lassie
	// Cat: rex
	// null: null
}
