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
	"testing"
)

// Three writer schemas used by the explicit union branch acceptance tests.
//
// schemaBranchSameShapeRecords is a union of two records that carry an
// identical field layout but have distinct Avro names. Their native Go values
// are the same map[string]interface{} shape, so a branch can never be guessed
// from the Go value type: it must be stated explicitly.
const schemaBranchSameShapeRecords = `[
  {"type":"record","name":"Person","fields":[{"name":"id","type":"int"}]},
  {"type":"record","name":"Robot","fields":[{"name":"id","type":"int"}]}
]`

// schemaBranchDefaultField wraps a ["string","null"] union inside a record
// field with a default value. It is used to prove that an explicitly selected
// "null" branch is encoded differently from a field that is simply omitted
// (which takes the first-branch default), and that union defaults are checked
// against the real rules of the first union member.
const schemaBranchDefaultField = `{
  "type":"record",
  "name":"Envelope",
  "fields":[
    {"name":"label","type":["string","null"],"default":"NONE"}
  ]
}`

// schemaBranchAnonymousPrimitives is a union of two anonymous members whose
// native Go representations collide: both "bytes" and a 4-byte "fixed" move as
// []byte. Explicit branch identity is required to select one.
const schemaBranchAnonymousPrimitives = `{
  "type":"record",
  "name":"Blob",
  "fields":[
    {"name":"payload","type":["bytes",{"type":"fixed","name":"Quad","size":4}]}
  ]
}`

// The same map value must encode with a different union discriminator when the
// branch is named explicitly, even though Person and Robot have the same
// structure and their Go values are indistinguishable.
func TestUnionBranchExplicitDistinguishesSameShapeRecords(t *testing.T) {
	codec, err := NewCodec(schemaBranchSameShapeRecords)
	if err != nil {
		t.Fatal(err)
	}

	person := map[string]interface{}{"id": int32(7)}

	personBuf, err := codec.BinaryFromNative(nil, NewUnionBranch("Person", person))
	if err != nil {
		t.Fatalf("explicit Person branch: %s", err)
	}
	robotBuf, err := codec.BinaryFromNative(nil, NewUnionBranch("Robot", person))
	if err != nil {
		t.Fatalf("explicit Robot branch: %s", err)
	}

	// Discriminator (first byte) is the only difference: 0 for Person, 2 for
	// Robot; the record payload is byte identical.
	if want := []byte{0x00, 0x0e}; !bytes.Equal(personBuf, want) {
		t.Errorf("Person encoding: GOT %#v; WANT %#v", personBuf, want)
	}
	if want := []byte{0x02, 0x0e}; !bytes.Equal(robotBuf, want) {
		t.Errorf("Robot encoding: GOT %#v; WANT %#v", robotBuf, want)
	}
	if bytes.Equal(personBuf, robotBuf) {
		t.Fatalf("explicit branch identity lost: %#v == %#v", personBuf, robotBuf)
	}

	// Short names and full names both resolve when unambiguous.
	fullBuf, err := codec.BinaryFromNative(nil, NewUnionBranch("Person", person))
	if err != nil {
		t.Fatalf("short name branch: %s", err)
	}
	if !bytes.Equal(fullBuf, personBuf) {
		t.Errorf("full and short branch names ought to encode identically")
	}

	// Decoded form carries the branch identity explicitly.
	out, remaining, err := codec.NativeFromBinary(robotBuf)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Errorf("unexpected remaining bytes: %#v", remaining)
	}
	sel, ok := UnionBranchFromNative(out)
	if !ok {
		t.Fatalf("decoded datum is not a single-branch union: %#v", out)
	}
	if sel.Branch != "Robot" {
		t.Errorf("decoded branch: GOT %q; WANT %q", sel.Branch, "Robot")
	}
	if dv, _ := sel.Datum.(map[string]interface{})["id"]; dv != int32(7) {
		t.Errorf("decoded value: GOT %#v; WANT int32(7)", sel.Datum)
	}
}

// A branch that is not a member of the writer schema must fail rather than
// fall back to guessing from the Go value.
func TestUnionBranchExplicitUnknownBranchFails(t *testing.T) {
	codec, err := NewCodec(schemaBranchSameShapeRecords)
	if err != nil {
		t.Fatal(err)
	}

	// Name absent from the union, with a perfectly valid record value.
	if _, err := codec.BinaryFromNative(nil, NewUnionBranch("Alien", map[string]interface{}{"id": int32(1)})); err == nil {
		t.Fatal("expected error when branch is not a writer-schema member")
	}

	// Primitive the union does not declare.
	if _, err := codec.BinaryFromNative(nil, NewUnionBranch("string", "x")); err == nil {
		t.Fatal("expected error selecting undeclared string branch")
	}

	// A non-nil value carried on the null branch must fail, and so must a nil
	// value carried on a non-null branch.
	nullUnion, err := NewCodec(`["null","int"]`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nullUnion.BinaryFromNative(nil, NewUnionBranch("null", 3)); err == nil {
		t.Fatal("expected error encoding non-nil value as null branch")
	}
	if _, err := nullUnion.BinaryFromNative(nil, NewUnionBranch("int", nil)); err == nil {
		t.Fatal("expected error encoding nil value as int branch")
	}
}

// Explicitly choosing "null" must produce different binary data than simply
// omitting the field, which takes the first member's default ("string").
func TestUnionBranchNullDiffersFromMissingField(t *testing.T) {
	codec, err := NewCodec(schemaBranchDefaultField)
	if err != nil {
		t.Fatal(err)
	}

	// Field omitted entirely: first branch "string" with default "NONE".
	omitted, err := codec.BinaryFromNative(nil, map[string]interface{}{})
	if err != nil {
		t.Fatalf("omitted field: %s", err)
	}
	// Explicit null: discriminator selects second member; label payload empty.
	explicitNull, err := codec.BinaryFromNative(
		nil,
		map[string]interface{}{"label": NewUnionBranch("null", nil)},
	)
	if err != nil {
		t.Fatalf("explicit null: %s", err)
	}
	// Explicit first branch for comparison.
	explicitString, err := codec.BinaryFromNative(
		nil,
		map[string]interface{}{"label": NewUnionBranch("string", "NONE")},
	)
	if err != nil {
		t.Fatalf("explicit string: %s", err)
	}

	// omitted: string discriminator 0x00 + length 4 zig-zag 0x08 + "NONE"
	if want := []byte{0x00, 0x08, 'N', 'O', 'N', 'E'}; !bytes.Equal(omitted, want) {
		t.Errorf("omitted field: GOT %#v; WANT %#v", omitted, want)
	}
	// explicit null: null discriminator 0x02, no payload
	if want := []byte{0x02}; !bytes.Equal(explicitNull, want) {
		t.Errorf("explicit null: GOT %#v; WANT %#v", explicitNull, want)
	}
	if bytes.Equal(omitted, explicitNull) {
		t.Fatal("null branch and missing/default field must encode differently")
	}
	if !bytes.Equal(omitted, explicitString) {
		t.Errorf("omitted field ought to equal explicit first-branch default: %#v vs %#v", omitted, explicitString)
	}

	// Round trip the explicit null and read the branch identity back.
	decoded, remaining, err := codec.NativeFromBinary(explicitNull)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Errorf("unexpected remaining bytes: %#v", remaining)
	}
	label := decoded.(map[string]interface{})["label"]
	if label != nil {
		t.Errorf("explicit null ought to decode to nil label; got %#v", label)
	}
}

// Default values are checked against the real rules of the union's first
// member.
func TestUnionBranchDefaultCheckedAgainstFirstMember(t *testing.T) {
	// Valid first-branch ("string") default builds and omits cleanly.
	if _, err := NewCodec(schemaBranchDefaultField); err != nil {
		t.Fatalf("valid first-branch default rejected: %s", err)
	}

	// A default that only fits the second member ("null") must be rejected
	// because defaults follow the first member's rules ("string").
	if _, err := NewCodec(`{
	  "type":"record","name":"BadDefault",
	  "fields":[{"name":"label","type":["string","null"],"default":null}]
	}`); err == nil {
		t.Fatal("expected schema error: null default does not fit first string member")
	}

	// When the first member is "null", a null default is legal and omitting
	// the field selects it.
	nullFirst, err := NewCodec(`{
	  "type":"record","name":"NullFirst",
	  "fields":[{"name":"label","type":["null","string"],"default":null}]
	}`)
	if err != nil {
		t.Fatalf("null-first default rejected: %s", err)
	}
	buf, err := nullFirst.BinaryFromNative(nil, map[string]interface{}{})
	if err != nil {
		t.Fatalf("omitted null-first field: %s", err)
	}
	if want := []byte{0x00}; !bytes.Equal(buf, want) {
		t.Errorf("null-first default: GOT %#v; WANT %#v", buf, want)
	}

	// A default with the wrong Go type for the first member fails its real
	// validation rules (string member requires a JSON string).
	if _, err := NewCodec(`{
	  "type":"record","name":"WrongTypeDefault",
	  "fields":[{"name":"label","type":["string","null"],"default":42}]
	}`); err == nil {
		t.Fatal("expected schema error: number default does not fit string member")
	}

	// Bytes default must be a JSON string; a number is rejected against the
	// first anonymous primitive member "bytes".
	if _, err := NewCodec(`{
	  "type":"record","name":"BadBytesDefault",
	  "fields":[{"name":"payload","type":["bytes",{"type":"fixed","name":"Quad2","size":4}],"default":12}]
	}`); err == nil {
		t.Fatal("expected schema error: number default does not fit first bytes member")
	}
}

// Two anonymous members sharing a Go representation (bytes and fixed are both
// []byte) must be selected explicitly.
func TestUnionBranchExplicitDistinguishesAnonymousPrimitives(t *testing.T) {
	codec, err := NewCodec(schemaBranchAnonymousPrimitives)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte{'A', 'B', 'C', 'D'}

	bytesBuf, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"payload": NewUnionBranch("bytes", value),
	})
	if err != nil {
		t.Fatalf("bytes branch: %s", err)
	}
	fixedBuf, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"payload": NewUnionBranch("Quad", value),
	})
	if err != nil {
		t.Fatalf("fixed branch: %s", err)
	}

	// bytes: discriminator 0, length prefix 8, then 4 bytes.
	if want := []byte{0x00, 0x08, 'A', 'B', 'C', 'D'}; !bytes.Equal(bytesBuf, want) {
		t.Errorf("bytes branch: GOT %#v; WANT %#v", bytesBuf, want)
	}
	// fixed: discriminator 2, then exactly 4 bytes with no length prefix.
	if want := []byte{0x02, 'A', 'B', 'C', 'D'}; !bytes.Equal(fixedBuf, want) {
		t.Errorf("fixed branch: GOT %#v; WANT %#v", fixedBuf, want)
	}
	if bytes.Equal(bytesBuf, fixedBuf) {
		t.Fatal("bytes and fixed branches must encode differently for the same []byte")
	}

	// A fixed value of the wrong size must fail the fixed member's real rules,
	// and must not silently be retried against the bytes branch.
	if _, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"payload": NewUnionBranch("Quad", []byte{'A', 'B'}),
	}); err == nil {
		t.Fatal("expected error encoding 2-byte value into 4-byte fixed branch")
	}
}

// On failure the caller's buffer must not gain partial union bytes: neither the
// discriminator nor any member fragment may be published.
func TestUnionBranchFailurePublishesNoPartialBinary(t *testing.T) {
	codec, err := NewCodec(`["string",{"type":"fixed","name":"Quad3","size":4}]`)
	if err != nil {
		t.Fatal(err)
	}

	prefix := []byte{0xAA, 0xBB}

	// Unknown branch: prefix returned unchanged.
	out, err := codec.BinaryFromNative(append([]byte(nil), prefix...), NewUnionBranch("nope", 1))
	if err == nil {
		t.Fatal("expected error for unknown branch")
	}
	if !bytes.Equal(out, prefix) {
		t.Errorf("unknown branch partial write: GOT %#v; WANT %#v", out, prefix)
	}

	// Failing member value (wrong fixed width): prefix returned unchanged with
	// no discriminator leaked.
	out, err = codec.BinaryFromNative(append([]byte(nil), prefix...), NewUnionBranch("Quad3", []byte{1, 2}))
	if err == nil {
		t.Fatal("expected error for short fixed value")
	}
	if !bytes.Equal(out, prefix) {
		t.Errorf("failing member partial write: GOT %#v; WANT %#v", out, prefix)
	}

	// Same guarantee through the dynamic single-key map interface.
	out, err = codec.BinaryFromNative(append([]byte(nil), prefix...), Union("Quad3", []byte{1, 2}))
	if err == nil {
		t.Fatal("expected error for short fixed value via dynamic map")
	}
	if !bytes.Equal(out, prefix) {
		t.Errorf("dynamic map partial write: GOT %#v; WANT %#v", out, prefix)
	}
}

// The ordinary dynamic selection interface (nil plus Union(name,value) map)
// keeps working exactly as before.
func TestUnionBranchDynamicInterfacePreserved(t *testing.T) {
	codec, err := NewCodec(`["null","string","int"]`)
	if err != nil {
		t.Fatal(err)
	}

	testBinaryCodecPass(t, `["null","string","int"]`, Union("null", nil), []byte{0x00})
	testBinaryCodecPass(t, `["null","string","int"]`, Union("string", "x"), []byte{0x02, 0x02, 'x'})
	testBinaryCodecPass(t, `["null","string","int"]`, Union("int", 3), []byte{0x04, 0x06})

	// Bare nil still selects the null branch.
	buf, err := codec.BinaryFromNative(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0x00}; !bytes.Equal(buf, want) {
		t.Errorf("bare nil: GOT %#v; WANT %#v", buf, want)
	}

	// A bare, unwrapped Go value remains rejected (no guessing).
	if _, err := codec.BinaryFromNative(nil, 3); err == nil {
		t.Fatal("expected rejection of unwrapped int value")
	}

	// Explicit and dynamic forms of the same member produce identical bytes.
	dyn, err := codec.BinaryFromNative(nil, Union("string", "x"))
	if err != nil {
		t.Fatal(err)
	}
	exp, err := codec.BinaryFromNative(nil, NewUnionBranch("string", "x"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dyn, exp) {
		t.Errorf("dynamic %#v vs explicit %#v ought to match", dyn, exp)
	}
}

// Explicit branch selection works through the textual (Avro JSON) encoder and
// reports the same identity on decode.
func TestUnionBranchTextualRoundTrip(t *testing.T) {
	codec, err := NewCodec(schemaBranchSameShapeRecords)
	if err != nil {
		t.Fatal(err)
	}

	text, err := codec.TextualFromNative(nil, NewUnionBranch("Robot", map[string]interface{}{"id": int32(4)}))
	if err != nil {
		t.Fatalf("textual explicit branch: %s", err)
	}
	if want := `{"Robot":{"id":4}}`; string(text) != want {
		t.Errorf("textual: GOT %s; WANT %s", text, want)
	}

	back, remaining, err := codec.NativeFromTextual(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Errorf("unexpected remaining text: %q", remaining)
	}
	sel, ok := UnionBranchFromNative(back)
	if !ok || sel.Branch != "Robot" {
		t.Fatalf("decoded branch identity wrong: %#v ok=%v", back, ok)
	}

	// Explicit null branch through a null-bearing union.
	nullCodec, err := NewCodec(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	nb, err := nullCodec.TextualFromNative(nil, NewUnionBranch("null", nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(nb) != "null" {
		t.Errorf("textual null: GOT %s; WANT null", nb)
	}
}

// Explicit branch selection also works for the two-way standard/wild JSON
// codec, which emits the bare value rather than the type-wrapped object.
func TestUnionBranchStandardJSONExplicit(t *testing.T) {
	codec, err := NewCodecForStandardJSONFull(schemaBranchSameShapeRecords)
	if err != nil {
		t.Fatal(err)
	}

	person := map[string]interface{}{"id": int32(9)}
	p, err := codec.TextualFromNative(nil, NewUnionBranch("Person", person))
	if err != nil {
		t.Fatalf("standard json Person branch: %s", err)
	}
	r, err := codec.TextualFromNative(nil, NewUnionBranch("Robot", person))
	if err != nil {
		t.Fatalf("standard json Robot branch: %s", err)
	}
	// Same Go value, explicit branch still selects; wild JSON output happens to
	// look identical here, but both declared branches encode without guessing.
	if want := `{"id":9}`; string(p) != want || string(r) != want {
		t.Errorf("standard json explicit: GOT %q / %q; WANT %q", p, r, want)
	}

	if _, err := codec.TextualFromNative(nil, NewUnionBranch("Alien", person)); err == nil {
		t.Fatal("expected error selecting undeclared branch in standard json codec")
	}
}
