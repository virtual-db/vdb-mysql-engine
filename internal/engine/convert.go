// Package engine — convert.go
//
// Conversions between protocol messages and core types.
package engine

import (
	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"
	"time"

	core "github.com/virtual-db/vdb-core"

	"github.com/virtual-db/vdb-mysql-engine/internal/innodb"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

func toRow(r *protocol.Row) core.Row {
	if r == nil {
		return nil
	}
	out := make(core.Row, len(r.Values))
	for i, v := range r.Values {
		if v == nil || v.Null {
			out[i] = core.Value{Null: true}
		} else {
			out[i] = core.Value{B: v.B}
		}
	}
	return out
}

func fromRow(r core.Row) *protocol.Row {
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

func toImages(im *protocol.Images) []core.KeyImage {
	if im == nil {
		return nil
	}
	out := make([]core.KeyImage, len(im.Images))
	for i, b := range im.Images {
		if len(b) > 0 {
			out[i] = b
		}
	}
	return out
}

func fromImages(ims []core.KeyImage) *protocol.Images {
	out := &protocol.Images{Images: make([][]byte, len(ims))}
	for i, b := range ims {
		out.Images[i] = b
	}
	return out
}

func toKey(k *protocol.Key) core.Key {
	if k == nil {
		return core.Key{}
	}
	return core.Key{Image: k.Image, Raw: k.Raw}
}

func toRef(r *protocol.RowRef) core.RowRef {
	if r == nil {
		return core.RowRef{}
	}
	ref := core.RowRef{PK: toKey(r.Pk), SourceKey: r.SourceKey, Whole: toRow(r.Whole)}
	if r.Origin == protocol.Origin_ORIGIN_DELTA {
		ref.Origin = core.OriginDelta
	}
	return ref
}

func fromRef(r core.RowRef) *protocol.RowRef {
	out := &protocol.RowRef{Pk: &protocol.Key{Image: r.PK.Image, Raw: r.PK.Raw}, SourceKey: r.SourceKey}
	if r.Origin == core.OriginDelta {
		out.Origin = protocol.Origin_ORIGIN_DELTA
	}
	if r.Whole != nil {
		out.Whole = fromRow(r.Whole)
	}
	return out
}

func toRange(r *protocol.Range) core.KeyRange {
	kr := core.KeyRange{}
	if r.Low != nil {
		kr.Images.Low, kr.RawLow, kr.LowParts = r.Low.Image, r.Low.Raw, uint16(r.LowParts)
		kr.Images.LowIncl, kr.RawLowIncl = r.LowIncl, r.LowIncl
	}
	if r.High != nil {
		kr.Images.High, kr.RawHigh, kr.HighParts = r.High.Image, r.High.Raw, uint16(r.HighParts)
		kr.Images.HighIncl, kr.RawHighIncl = r.HighIncl, r.HighIncl
	}
	return kr
}

func toCols(c []uint32) core.ColumnSet {
	out := make(core.ColumnSet, len(c))
	for i, x := range c {
		out[i] = uint16(x)
	}
	return out
}

func toStmtContext(m *protocol.StmtBegin, iso innodb.Isolation) core.StmtContext {
	eval := core.EvalContext{}
	for _, s := range m.Eval {
		eval[s.Name] = s.Value
	}
	return core.StmtContext{Database: m.Database, Snapshot: iso.Scope(), View: iso.View(),
		LockWait: time.Duration(m.LockWaitMs) * time.Millisecond, Eval: eval, ReadOnly: m.ReadOnly, Digest: m.Digest}
}

// FromBatch is fromBatch, for tests.
func FromBatch(b core.Batch, end bool) *protocol.Batch { return fromBatch(b, end) }

// fromBatch builds the protocol form of a batch. The messages are only
// marshalled and dropped, so each kind comes from one allocation per batch
// instead of one per row and per column; the bytes are not copied.
func fromBatch(b core.Batch, end bool) *protocol.Batch {
	out := &protocol.Batch{End: end}
	if len(b.Rows) > 0 {
		nv := 0
		for i, r := range b.Rows {
			nv += len(r)
			if i < len(b.Refs) {
				nv += len(b.Refs[i].Whole)
			}
		}
		vals := make([]protocol.Value, nv)
		vptr := make([]*protocol.Value, nv)
		for i := range vals {
			vptr[i] = &vals[i]
		}
		nr := len(b.Rows)
		if len(b.Refs) < nr {
			nr = len(b.Refs)
		}
		rows := make([]protocol.Row, len(b.Rows), len(b.Rows)+nr)
		out.Rows = make([]*protocol.Row, len(b.Rows))
		fill := func(row *protocol.Row, r core.Row) {
			row.Values, vptr = vptr[:len(r):len(r)], vptr[len(r):]
			for j, v := range r {
				if v.Null {
					row.Values[j].Null = true
				} else {
					row.Values[j].B = v.B
				}
			}
		}
		for i, r := range b.Rows {
			fill(&rows[i], r)
			out.Rows[i] = &rows[i]
		}
		if nr > 0 {
			refs := make([]protocol.RowRef, nr)
			keys := make([]protocol.Key, nr)
			out.Refs = make([]*protocol.RowRef, nr)
			for i := range refs {
				r := b.Refs[i]
				keys[i].Image, keys[i].Raw = r.PK.Image, r.PK.Raw
				refs[i].Pk, refs[i].SourceKey = &keys[i], r.SourceKey
				if r.Origin == core.OriginDelta {
					refs[i].Origin = protocol.Origin_ORIGIN_DELTA
				}
				if r.Whole != nil {
					rows = rows[:len(rows)+1] // within the capacity: no row moves
					w := &rows[len(rows)-1]
					fill(w, r.Whole)
					refs[i].Whole = w
				}
				out.Refs[i] = &refs[i]
			}
		}
	}
	if len(b.KeyIdx) > 0 {
		out.KeyIdx = append([]uint32(nil), b.KeyIdx...)
	}
	return out
}

func kindOf(k core.TableKind) protocol.TableKind {
	switch k {
	case core.Local:
		return protocol.TableKind_KIND_LOCAL
	case core.Detached:
		return protocol.TableKind_KIND_DETACHED
	case core.Provided:
		return protocol.TableKind_KIND_PROVIDED
	}
	return protocol.TableKind_KIND_MIRRORED
}

func fetchOf(f core.FetchMode) protocol.FetchMode {
	switch f {
	case core.Default:
		return protocol.FetchMode_FETCH_DEFAULT
	case core.Virtual:
		return protocol.FetchMode_FETCH_VIRTUAL
	case core.Transformed:
		return protocol.FetchMode_FETCH_TRANSFORMED
	}
	return protocol.FetchMode_FETCH_SOURCE
}

func fkAction(a uint32) mysqldef.FKAction {
	switch a {
	case 1:
		return mysqldef.FKNoAction
	case 2:
		return mysqldef.FKCascade
	case 3:
		return mysqldef.FKSetNull
	}
	return mysqldef.FKRestrict
}
