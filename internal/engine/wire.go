// Package engine — wire.go
//
// Replies that carry rows are encoded straight from the core's rows into the
// reply frame, without building a protocol.Row and a protocol.Value per row
// and per column first. The bytes are exactly what proto.Marshal writes for
// the same message (fields in field-number order, zero values left out), so
// vdb.so sees no difference; tests compare the two on random batches.
package engine

import (
	"encoding/binary"

	core "github.com/virtual-db/vdb-core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

// Field numbers of vdb.proto used below.
const (
	fRowValues    = 1 // Row.values
	fValueNull    = 1 // Value.null
	fValueB       = 2 // Value.b
	fKeyRaw       = 1 // Key.raw
	fKeyImage     = 2 // Key.image
	fRefOrigin    = 1 // RowRef.origin
	fRefPK        = 2 // RowRef.pk
	fRefSourceKey = 3 // RowRef.source_key
	fRefWhole     = 4 // RowRef.whole
	fBatchRows    = 1 // Batch.rows
	fBatchRefs    = 2 // Batch.refs
	fBatchKeyIdx  = 3 // Batch.key_idx (packed)
	fBatchEnd     = 4 // Batch.end
)

// wireBatch is a batch to encode as a protocol.Batch: the rows, the refs that
// go with them (as fromBatch: at most one per row) and the sizes measured
// before writing, so the frame is allocated once.
type wireBatch struct {
	field   protowire.Number // the reply's field that holds the batch
	b       core.Batch
	end     bool
	nrefs   int
	rowSize []int // encoded size of each row
	refSize []int // encoded size of each ref
	idxSize int   // packed key_idx payload
	size    int   // the Batch message
}

func newWireBatch(field protowire.Number, b core.Batch, end bool) *wireBatch {
	w := &wireBatch{field: field, b: b, end: end}
	w.nrefs = len(b.Refs)
	if len(b.Rows) < w.nrefs {
		w.nrefs = len(b.Rows)
	}
	sizes := make([]int, len(b.Rows)+w.nrefs)
	w.rowSize, w.refSize = sizes[:len(b.Rows)], sizes[len(b.Rows):]
	for i, r := range b.Rows {
		n := rowSize(r)
		w.rowSize[i] = n
		w.size += 1 + protowire.SizeVarint(uint64(n)) + n
	}
	for i := 0; i < w.nrefs; i++ {
		n := refSize(&b.Refs[i])
		w.refSize[i] = n
		w.size += 1 + protowire.SizeVarint(uint64(n)) + n
	}
	for _, k := range b.KeyIdx {
		w.idxSize += protowire.SizeVarint(uint64(k))
	}
	if len(b.KeyIdx) > 0 {
		w.size += 1 + protowire.SizeVarint(uint64(w.idxSize)) + w.idxSize
	}
	if end {
		w.size += 2
	}
	return w
}

func valueSize(v core.Value) int {
	n := 0
	if v.Null {
		n += 2
	} else if len(v.B) > 0 {
		n += 1 + protowire.SizeBytes(len(v.B))
	}
	return n
}

func rowSize(r core.Row) int {
	n := 0
	for _, v := range r {
		s := valueSize(v)
		n += 1 + protowire.SizeVarint(uint64(s)) + s
	}
	return n
}

func bytesField(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return 1 + protowire.SizeBytes(len(b))
}

func keySize(k core.Key) int { return bytesField(k.Raw) + bytesField(k.Image) }

func refSize(r *core.RowRef) int {
	n := 0
	if r.Origin == core.OriginDelta {
		n += 1 + protowire.SizeVarint(uint64(protocol.Origin_ORIGIN_DELTA))
	}
	ks := keySize(r.PK)
	n += 1 + protowire.SizeVarint(uint64(ks)) + ks // pk is always set
	n += bytesField(r.SourceKey)
	if r.Whole != nil {
		ws := rowSize(r.Whole)
		n += 1 + protowire.SizeVarint(uint64(ws)) + ws
	}
	return n
}

// fieldSize is the size of the batch as a field of its reply.
func (w *wireBatch) fieldSize() int {
	return protowire.SizeTag(w.field) + protowire.SizeBytes(w.size)
}

func appendBytesField(buf []byte, f protowire.Number, b []byte) []byte {
	if len(b) == 0 {
		return buf
	}
	buf = protowire.AppendTag(buf, f, protowire.BytesType)
	return protowire.AppendBytes(buf, b)
}

func appendRow(buf []byte, r core.Row) []byte {
	for _, v := range r {
		buf = protowire.AppendTag(buf, fRowValues, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(valueSize(v)))
		if v.Null {
			buf = protowire.AppendTag(buf, fValueNull, protowire.VarintType)
			buf = append(buf, 1)
		} else {
			buf = appendBytesField(buf, fValueB, v.B)
		}
	}
	return buf
}

func (w *wireBatch) append(buf []byte) []byte {
	buf = protowire.AppendTag(buf, w.field, protowire.BytesType)
	buf = protowire.AppendVarint(buf, uint64(w.size))
	for i, r := range w.b.Rows {
		buf = protowire.AppendTag(buf, fBatchRows, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(w.rowSize[i]))
		buf = appendRow(buf, r)
	}
	for i := 0; i < w.nrefs; i++ {
		r := &w.b.Refs[i]
		buf = protowire.AppendTag(buf, fBatchRefs, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(w.refSize[i]))
		if r.Origin == core.OriginDelta {
			buf = protowire.AppendTag(buf, fRefOrigin, protowire.VarintType)
			buf = protowire.AppendVarint(buf, uint64(protocol.Origin_ORIGIN_DELTA))
		}
		buf = protowire.AppendTag(buf, fRefPK, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(keySize(r.PK)))
		buf = appendBytesField(buf, fKeyRaw, r.PK.Raw)
		buf = appendBytesField(buf, fKeyImage, r.PK.Image)
		buf = appendBytesField(buf, fRefSourceKey, r.SourceKey)
		if r.Whole != nil {
			buf = protowire.AppendTag(buf, fRefWhole, protowire.BytesType)
			buf = protowire.AppendVarint(buf, uint64(rowSize(r.Whole)))
			buf = appendRow(buf, r.Whole)
		}
	}
	if len(w.b.KeyIdx) > 0 {
		buf = protowire.AppendTag(buf, fBatchKeyIdx, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(w.idxSize))
		for _, k := range w.b.KeyIdx {
			buf = protowire.AppendVarint(buf, uint64(k))
		}
	}
	if w.end {
		buf = protowire.AppendTag(buf, fBatchEnd, protowire.VarintType)
		buf = append(buf, 1)
	}
	return buf
}

// framed is a reply whose frame is written by hand: head's fields, then the
// batches (each a field of the reply with a higher number than head's), then
// tail's fields (higher still). Any of head and tail may be nil. It is what
// proto.Marshal writes for the whole reply.
type framed struct {
	head    proto.Message
	batches []*wireBatch
	tail    proto.Message
}

// frame builds the reply frame of op, header included.
func (f *framed) frame(op protocol.Op) ([]byte, error) {
	var headSize, tailSize int
	if f.head != nil {
		headSize = proto.Size(f.head)
	}
	if f.tail != nil {
		tailSize = proto.Size(f.tail)
	}
	size := headSize + tailSize
	for _, w := range f.batches {
		size += w.fieldSize()
	}
	if size > protocol.MaxBody {
		return nil, protocol.ErrTooLarge
	}
	buf := make([]byte, 5, 5+size)
	// head and tail are built per request and not shared, so the sizes
	// proto.Size just cached stay valid for MarshalAppend.
	opts := proto.MarshalOptions{UseCachedSize: true}
	var err error
	if f.head != nil {
		if buf, err = opts.MarshalAppend(buf, f.head); err != nil {
			return nil, err
		}
	}
	for _, w := range f.batches {
		buf = w.append(buf)
	}
	if f.tail != nil {
		if buf, err = opts.MarshalAppend(buf, f.tail); err != nil {
			return nil, err
		}
	}
	buf[0] = byte(op.Reply())
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(buf)-5))
	return buf, nil
}

// FrameReply is framed.frame for tests: the reply frame of op with head's
// fields, the batches as fields fields[i], then tail's fields.
func FrameReply(op protocol.Op, head proto.Message, fields []protowire.Number, bs []core.Batch, ends []bool, tail proto.Message) ([]byte, error) {
	f := &framed{head: head, tail: tail}
	for i := range bs {
		f.batches = append(f.batches, newWireBatch(fields[i], bs[i], ends[i]))
	}
	return f.frame(op)
}
