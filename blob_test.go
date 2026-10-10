package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
	"unicode/utf16"
)

func TestToTargetBlob(t *testing.T) {
	src := `[{"c0":1,"c1":"Ünïcödé — 中文 テキスト","c2":"emoji 😀 and \"quotes\"","c3":"ø€"}]`
	src = strings.Repeat(src, 9000) // > 64 KB: characters cut at buffer boundaries
	var in bytes.Buffer
	zw := gzip.NewWriter(&in)
	zw.Write([]byte(src))
	zw.Close()
	out, err := toTargetBlob(in.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(zr)
	u := make([]uint16, len(raw)/2)
	for i := range u {
		u[i] = uint16(raw[2*i]) | uint16(raw[2*i+1])<<8
	}
	if got := string(utf16.Decode(u)); got != src {
		t.Fatalf("got %q", got)
	}
}
