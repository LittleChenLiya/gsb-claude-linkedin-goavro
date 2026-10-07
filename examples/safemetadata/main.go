// Command safemetadata is a small, in-memory acceptance fixture for the
// OCFConfig.MetaDataSafe option. It demonstrates:
//
//  1. Writing two custom metadata items, then mutating both the caller's
//     original byte slices and the byte slices returned by the writer, and
//     showing the already written header is unchanged.
//  2. Attempting to overwrite the reserved "avro.schema" key, which fails
//     before the header is written, leaving zero output bytes.
//  3. Stable, lexicographic ordering of ordinary custom keys across runs with
//     different map insertion orders.
//  4. That leaving MetaDataSafe unset keeps the original interface working.
//
// Run with: go run ./examples/safemetadata
package main

import (
	"bytes"
	"fmt"
	"os"

	goavro "github.com/linkedin/goavro/v2"
)

func main() {
	failures := 0
	check := func(name string, ok bool, detail string) {
		status := "PASS"
		if !ok {
			status = "FAIL"
			failures++
		}
		fmt.Printf("[%s] %s %s\n", status, name, detail)
	}

	const schema = `{"type":"long"}`
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	//
	// 1. Defensive copy: caller slices and returned slices cannot change the
	//    generated header.
	//
	alpha := []byte("alpha-value")
	beta := []byte("beta-value")
	custom := map[string][]byte{"alpha": alpha, "beta": beta}

	buf1 := new(bytes.Buffer)
	w1, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:            buf1,
		Schema:       schema,
		MetaData:     custom,
		MetaDataSafe: true,
		SyncMarker:   sync,
	})
	if err != nil {
		fmt.Println("[FAIL] create safe writer:", err)
		os.Exit(1)
	}
	headerBefore := append([]byte(nil), buf1.Bytes()...)

	// Tamper with the caller's input after the header has been generated.
	for i := range alpha {
		alpha[i] = 'X'
	}
	for i := range beta {
		beta[i] = 'Y'
	}
	custom["alpha"][0] = 'Z'

	// Tamper with what the writer hands back too.
	returned := w1.MetaData()
	for k := range returned {
		for i := range returned[k] {
			returned[k][i] = '!'
		}
	}

	check("defensive copy: header unchanged after slice mutation",
		bytes.Equal(buf1.Bytes(), headerBefore),
		fmt.Sprintf("(%d header bytes)", len(headerBefore)))

	r1, err := goavro.NewOCFReader(bytes.NewReader(append([]byte(nil), buf1.Bytes()...)))
	if err != nil {
		fmt.Println("[FAIL] create reader:", err)
		os.Exit(1)
	}
	check("defensive copy: original values readable back",
		string(r1.MetaData()["alpha"]) == "alpha-value" &&
			string(r1.MetaData()["beta"]) == "beta-value",
		fmt.Sprintf("(alpha=%q beta=%q)", r1.MetaData()["alpha"], r1.MetaData()["beta"]))

	//
	// 2. Reserved key rejected before any header byte is written.
	//
	buf2 := new(bytes.Buffer)
	_, err = goavro.NewOCFWriter(goavro.OCFConfig{
		W:            buf2,
		Schema:       schema,
		MetaData:     map[string][]byte{"avro.schema": []byte("not-allowed")},
		MetaDataSafe: true,
	})
	check("reserved key rejected", err != nil, fmt.Sprintf("(err=%v)", err))
	check("reserved key rejection produced zero output", buf2.Len() == 0,
		fmt.Sprintf("(%d bytes written)", buf2.Len()))

	// Same contract through the safe modification entry point.
	var cfg goavro.OCFConfig
	err = cfg.SetMetaData(map[string][]byte{"avro.codec": []byte("snappy")})
	check("SetMetaData rejects avro.codec", err != nil, fmt.Sprintf("(err=%v)", err))
	check("SetMetaData stored nothing on rejection", cfg.MetaData == nil, "")

	//
	// 3. Stable ordering regardless of map insertion order.
	//
	buildHeader := func(order ...string) []byte {
		values := map[string]string{"zulu": "1", "mike": "2", "alpha": "3"}
		md := make(map[string][]byte, len(order))
		for _, k := range order {
			md[k] = []byte(values[k])
		}
		bb := new(bytes.Buffer)
		if _, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W: bb, Schema: schema, MetaData: md, MetaDataSafe: true, SyncMarker: sync,
		}); err != nil {
			panic(err)
		}
		return bb.Bytes()
	}
	hA := buildHeader("alpha", "mike", "zulu")
	hB := buildHeader("zulu", "alpha", "mike")
	hC := buildHeader("mike", "zulu", "alpha")
	check("custom key order stable across insertion orders",
		bytes.Equal(hA, hB) && bytes.Equal(hB, hC),
		fmt.Sprintf("(%d identical header bytes)", len(hA)))
	fmt.Printf("       custom keys sorted within header: alpha < avro.codec < avro.schema < mike < zulu\n")

	//
	// 4. New option off: original interface still usable, reserved keys
	//    tolerated and overwritten.
	//
	buf4 := new(bytes.Buffer)
	w4, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:          buf4,
		Schema:     schema,
		MetaData:   map[string][]byte{"foo": []byte("BOING"), "avro.schema": []byte(`{"type":"int"}`)},
		SyncMarker: sync,
	})
	if err != nil {
		fmt.Println("[FAIL] legacy writer:", err)
		os.Exit(1)
	}
	r4, err := goavro.NewOCFReader(bytes.NewReader(append([]byte(nil), buf4.Bytes()...)))
	if err != nil {
		fmt.Println("[FAIL] legacy reader:", err)
		os.Exit(1)
	}
	check("legacy path: custom key round trips",
		string(r4.MetaData()["foo"]) == "BOING",
		fmt.Sprintf("(foo=%q)", r4.MetaData()["foo"]))
	check("legacy path: caller avro.schema overridden by writer",
		string(r4.MetaData()["avro.schema"]) == schema,
		fmt.Sprintf("(avro.schema=%q)", r4.MetaData()["avro.schema"]))
	check("writer MetaData() hides reserved keys",
		func() bool { _, ok := w4.MetaData()["avro.schema"]; return !ok }(),
		"")

	if failures > 0 {
		fmt.Printf("\n%d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nall checks passed")
}
