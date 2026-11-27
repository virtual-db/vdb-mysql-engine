// Package engine_test — tests/convert_test.go
//
// FromBatch builds its messages from one allocation per kind; the bytes it
// marshals to must be those of building one message per row and per column.
package engine_test

import (
	"bytes"
	"math/rand"
	"testing"

	core "github.com/virtual-db/vdb-core"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-db/vdb-mysql-engine/internal/engine"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

// refRow and refBatch are the one-message-per-value conversion FromBatch replaced.
func refRow(r core.Row) *protocol.Row {
	out := &protocol.Row{Values: make([]*protocol.Value, len(r))}
	for i, v := range r {
		if v.Null {
			out.Values[i] = &protocol.Value{Null: true}
		} else {
			out.Values[i] = &protocol.Value{B: v.B}
		}
	}
	return out
}

func refBatch(b core.Batch, end bool) *protocol.Batch {
	out := &protocol.Batch{End: end}
	for i, r := range b.Rows {
		out.Rows = append(out.Rows, refRow(r))
		if i < len(b.Refs) {
			ref := b.Refs[i]
			pr := &protocol.RowRef{Pk: &protocol.Key{Image: ref.PK.Image, Raw: ref.PK.Raw}, SourceKey: ref.SourceKey}
			if ref.Origin == core.OriginDelta {
				pr.Origin = protocol.Origin_ORIGIN_DELTA
			}
			if ref.Whole != nil {
				pr.Whole = refRow(ref.Whole)
			}
			out.Refs = append(out.Refs, pr)
		}
	}
	for _, k := range b.KeyIdx {
		out.KeyIdx = append(out.KeyIdx, k)
	}
	return out
}

func randBytes(r *rand.Rand) []byte {
	switch r.Intn(4) {
	case 0:
		return nil
	case 1:
		return []byte{}
	}
	b := make([]byte, r.Intn(40))
	r.Read(b)
	return b
}

func randRow(r *rand.Rand, n int) core.Row {
	row := make(core.Row, n)
	for i := range row {
		if r.Intn(5) == 0 {
			row[i] = core.Value{Null: true}
		} else {
			row[i] = core.Value{B: randBytes(r)}
		}
	}
	return row
}

func randBatch(r *rand.Rand) core.Batch {
	var b core.Batch
	rows, cols := r.Intn(30), r.Intn(6)
	for i := 0; i < rows; i++ {
		b.Rows = append(b.Rows, randRow(r, cols))
	}
	refs := rows
	switch r.Intn(4) {
	case 0:
		refs = 0
	case 1:
		refs = r.Intn(rows + 3) // fewer or more refs than rows
	}
	for i := 0; i < refs; i++ {
		ref := core.RowRef{PK: core.Key{Image: randBytes(r), Raw: randBytes(r)}, SourceKey: randBytes(r)}
		if r.Intn(2) == 0 {
			ref.Origin = core.OriginDelta
		}
		if r.Intn(3) == 0 {
			ref.Whole = randRow(r, r.Intn(5))
		}
		b.Refs = append(b.Refs, ref)
	}
	if r.Intn(2) == 0 {
		for i := 0; i < rows; i++ {
			b.KeyIdx = append(b.KeyIdx, uint32(r.Intn(10)))
		}
	}
	return b
}

func TestFromBatchMatchesPerValueMessages(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		b, end := randBatch(r), r.Intn(2) == 0
		want, err := proto.MarshalOptions{Deterministic: true}.Marshal(refBatch(b, end))
		if err != nil {
			t.Fatal(err)
		}
		gotMsg := engine.FromBatch(b, end)
		got, err := proto.MarshalOptions{Deterministic: true}.Marshal(gotMsg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("batch %d: marshalled bytes differ", i)
		}
		if !proto.Equal(gotMsg, refBatch(b, end)) {
			t.Fatalf("batch %d: messages differ", i)
		}
	}
}

func BenchmarkFromBatch(b *testing.B) {
	r := rand.New(rand.NewSource(2))
	var batch core.Batch
	for i := 0; i < 4096; i++ {
		batch.Rows = append(batch.Rows, randRow(r, 2))
		batch.Refs = append(batch.Refs, core.RowRef{PK: core.Key{Image: randBytes(r)}, SourceKey: randBytes(r)})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := protocol.Encode(&buf, protocol.OpScanNext, &protocol.ScanNextReply{Batch: engine.FromBatch(batch, false)}); err != nil {
			b.Fatal(err)
		}
	}
}
