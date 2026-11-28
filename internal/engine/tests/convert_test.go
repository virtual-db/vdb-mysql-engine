// Package engine_test — tests/convert_test.go
//
// Replies that carry rows are written straight from core rows; their frames
// must be the frames of the same replies built as protocol messages.
package engine_test

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	core "github.com/virtual-db/vdb-core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-db/vdb-mysql-engine/internal/engine"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

// refRow and refBatch are the one-message-per-value conversion the framed
// replies replaced.
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

func encodeFrame(t *testing.T, op protocol.Op, msg proto.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, op.Reply(), msg); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func randWarnings(r *rand.Rand) []*protocol.Warning {
	var ws []*protocol.Warning
	for i := r.Intn(3); i > 0; i-- {
		ws = append(ws, &protocol.Warning{Code: uint32(r.Intn(2000)), Text: strconv.Itoa(r.Int())})
	}
	return ws
}

func randImages(r *rand.Rand) [][]byte {
	var ims [][]byte
	for i := r.Intn(3); i > 0; i-- {
		ims = append(ims, randBytes(r))
	}
	return ims
}

// The hand-written reply frames must be the frames protocol.Encode writes for
// the same replies built one message per row and per column (refBatch, the
// conversion they replaced).
func TestFramedRepliesMatchEncode(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 3000; i++ {
		b1, b2 := randBatch(r), randBatch(r)
		e1, e2 := r.Intn(2) == 0, r.Intn(2) == 0

		// ScanNextReply: batch only.
		got, err := engine.FrameReply(protocol.OpScanNext, nil, []protowire.Number{1}, []core.Batch{b1}, []bool{e1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := encodeFrame(t, protocol.OpScanNext, &protocol.ScanNextReply{Batch: refBatch(b1, e1)}); !bytes.Equal(got, want) {
			t.Fatalf("%d: ScanNextReply frame differs", i)
		}

		// ScanOpenReply, unordered (merged) and ordered (source, delta).
		scan := uint32(r.Intn(1000))
		ws, ims := randWarnings(r), randImages(r)
		head, tail := &protocol.ScanOpenReply{Scan: scan}, &protocol.ScanOpenReply{ShadowImages: ims, Warnings: ws}
		got, err = engine.FrameReply(protocol.OpScanOpen, head, []protowire.Number{2}, []core.Batch{b1}, []bool{e1}, tail)
		if err != nil {
			t.Fatal(err)
		}
		want := encodeFrame(t, protocol.OpScanOpen, &protocol.ScanOpenReply{Scan: scan, Merged: refBatch(b1, e1), ShadowImages: ims, Warnings: ws})
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: unordered ScanOpenReply frame differs", i)
		}
		got, err = engine.FrameReply(protocol.OpScanOpen, head, []protowire.Number{3, 4}, []core.Batch{b1, b2}, []bool{e1, e2}, tail)
		if err != nil {
			t.Fatal(err)
		}
		want = encodeFrame(t, protocol.OpScanOpen, &protocol.ScanOpenReply{Scan: scan, Source: refBatch(b1, e1), Delta: refBatch(b2, e2), ShadowImages: ims, Warnings: ws})
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: ordered ScanOpenReply frame differs", i)
		}

		// LookupReply: batch, then continuation, shadow images, warnings.
		rest := &protocol.LookupReply{Continuation: randBytes(r), ShadowImages: ims, Warnings: ws}
		got, err = engine.FrameReply(protocol.OpLookup, nil, []protowire.Number{1}, []core.Batch{b1}, []bool{e1}, rest)
		if err != nil {
			t.Fatal(err)
		}
		want = encodeFrame(t, protocol.OpLookup, &protocol.LookupReply{Batch: refBatch(b1, e1), Continuation: rest.Continuation, ShadowImages: ims, Warnings: ws})
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: LookupReply frame differs", i)
		}
	}
}

// benchBatch is a full batch of narrow rows (4,096 rows of 2 columns).
func benchBatch() core.Batch {
	r := rand.New(rand.NewSource(2))
	var batch core.Batch
	for i := 0; i < 4096; i++ {
		batch.Rows = append(batch.Rows, randRow(r, 2))
		batch.Refs = append(batch.Refs, core.RowRef{PK: core.Key{Image: randBytes(r)}, SourceKey: randBytes(r)})
	}
	return batch
}

// BenchmarkMessageScanNext is the reply as it was built before: messages, then Encode.
func BenchmarkMessageScanNext(b *testing.B) {
	batch := benchBatch()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := protocol.Encode(&buf, protocol.OpScanNext.Reply(), &protocol.ScanNextReply{Batch: refBatch(batch, false)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFramedScanNext(b *testing.B) {
	batch := benchBatch()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := engine.FrameReply(protocol.OpScanNext, nil, []protowire.Number{1}, []core.Batch{batch}, []bool{false}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// TestMarkedBatchMatchesEncode: RowRef.new_lock written by hand is what
// proto.Marshal writes (handoff #21).
func TestMarkedBatchMatchesEncode(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	modes := []protocol.LockMode{protocol.LockMode_LOCK_NONE, protocol.LockMode_LOCK_SHARED, protocol.LockMode_LOCK_EXCLUSIVE}
	for i := 0; i < 2000; i++ {
		b, end := randBatch(r), r.Intn(2) == 0
		var marks []protocol.LockMode
		if r.Intn(4) != 0 {
			marks = make([]protocol.LockMode, len(b.Refs))
			for j := range marks {
				marks[j] = modes[r.Intn(len(modes))]
			}
		}
		got, err := engine.FrameMarkedBatch(b, end, marks)
		if err != nil {
			t.Fatal(err)
		}
		want := refBatch(b, end)
		for j, ref := range want.Refs {
			if j < len(marks) {
				ref.NewLock = marks[j]
			}
		}
		if w := encodeFrame(t, protocol.OpScanNext, &protocol.ScanNextReply{Batch: want}); !bytes.Equal(got, w) {
			t.Fatalf("%d: marked batch frame differs", i)
		}
	}
}
