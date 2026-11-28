// Package source_test — tests/copyrow_test.go
//
// Driver rows are copied out of the driver's buffers: NULL stays NULL, an
// empty string stays a non-NULL empty value, and each value is capped so an
// append to it cannot write into the next one.
package source_test

import (
	"bytes"
	"database/sql"
	"testing"

	. "github.com/virtual-db/vdb-mysql-engine/internal/source"
)

func TestCopyRows(t *testing.T) {
	buf := []byte("abcdef")
	rows := [][]sql.RawBytes{
		{nil, sql.RawBytes{}, sql.RawBytes(buf[0:3]), sql.RawBytes(buf[3:6])},
		{sql.RawBytes("x"), nil},
	}
	got := CopyRows(rows)
	buf[0] = 'Z' // the driver reuses its buffer after Next
	r := got[0]
	if !r[0].Null || r[0].B != nil {
		t.Fatalf("NULL: got %+v", r[0])
	}
	if r[1].Null || r[1].B != nil {
		t.Fatalf("empty string: got %+v, want a non-NULL nil value as before", r[1])
	}
	if r[2].Null || !bytes.Equal(r[2].B, []byte("abc")) || !bytes.Equal(r[3].B, []byte("def")) {
		t.Fatalf("values: got %q %q", r[2].B, r[3].B)
	}
	if cap(r[2].B) != 3 {
		t.Fatalf("value not capped: cap %d", cap(r[2].B))
	}
	_ = append(r[2].B, '!')
	if !bytes.Equal(r[3].B, []byte("def")) {
		t.Fatalf("append to one value changed the next: %q", r[3].B)
	}
	if len(r) != 4 || cap(r) != 4 {
		t.Fatalf("row not capped: len %d cap %d", len(r), cap(r))
	}
	if len(got[1]) != 2 || string(got[1][0].B) != "x" || !got[1][1].Null {
		t.Fatalf("second row: %+v", got[1])
	}
}
