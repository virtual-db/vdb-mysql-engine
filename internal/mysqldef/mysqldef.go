// Package mysqldef holds the MySQL definitions the engine keeps in vdb-core's
// catalog (implementation plan doc 09 §3.6, D-88). The core stores each
// catalog entry's engine part as opaque bytes; this package encodes MySQL's
// part (column types, charsets and collations, key-part lengths, foreign
// keys, CREATE TABLE text, the Source's definition and AUTO_INCREMENT, local
// changes' and objects' statements) and decodes it, with a cache so hot paths
// such as literal rendering decode a column once.
package mysqldef

import (
	"encoding/json"
	"strings"
	"sync"

	core "github.com/virtual-db/vdb-core"
)

// Column is a column's MySQL part.
type Column struct {
	Type          string `json:"type,omitempty"` // as mysqld prints it, e.g. "varchar(20)"
	Charset       string `json:"cs,omitempty"`
	Collation     string `json:"coll,omitempty"`
	Timestamp     bool   `json:"ts,omitempty"`    // TIMESTAMP columns travel as Unix time
	Float         bool   `json:"float,omitempty"` // FLOAT and DOUBLE columns are never pushed down
	AutoIncrement bool   `json:"ai,omitempty"`
}

// Index is an index's MySQL part.
type Index struct {
	PartLens []uint32 `json:"parts,omitempty"` // sort-image length of each key part
}

// FKAction is a foreign-key referential action.
type FKAction uint8

const (
	FKRestrict FKAction = iota
	FKNoAction
	FKCascade
	FKSetNull
)

// ForeignKey is one foreign key declared on a child table.
type ForeignKey struct {
	Name          string       `json:"name"`
	Columns       []uint16     `json:"cols"` // child columns
	ChildIndex    core.IndexID `json:"cidx"` // the child's supporting index
	ParentDB      string       `json:"pdb"`
	ParentTable   string       `json:"ptab"`
	ParentColumns []string     `json:"pcols"`
	OnDelete      FKAction     `json:"del"`
	OnUpdate      FKAction     `json:"upd"`
}

// Table is a table's MySQL part.
type Table struct {
	Definition  string       `json:"def,omitempty"` // CREATE TABLE exactly as mysqld prints it
	ForeignKeys []ForeignKey `json:"fks,omitempty"`
}

// Source is a Source mapping's MySQL part.
type Source struct {
	Definition string `json:"def,omitempty"` // the Source's SHOW CREATE TABLE
	AutoInc    uint64 `json:"ai,omitempty"`  // the Source's AUTO_INCREMENT at discovery (D-15)
	RowsEst    uint64 `json:"rows,omitempty"`
}

// Local is a local change's MySQL part.
type Local struct {
	Statement string `json:"stmt,omitempty"`
}

// Object is a view's, routine's or trigger's MySQL part.
type Object struct {
	Statement string `json:"stmt,omitempty"`
	SQLMode   string `json:"mode,omitempty"`
	Definer   string `json:"definer,omitempty"`
}

// Object kinds, as the core stores them.
const (
	View      = "view"
	Procedure = "procedure"
	Function  = "function"
	Trigger   = "trigger"
)

// Encode returns the bytes of a MySQL part.
func Encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("mysqldef: " + err.Error())
	}
	return b
}

var (
	mu    sync.RWMutex
	cache = map[string]any{}
)

// decode decodes b into a T, caching by content.
func decode[T any](b []byte) T {
	var zero T
	if len(b) == 0 {
		return zero
	}
	mu.RLock()
	v, ok := cache[string(b)]
	mu.RUnlock()
	if ok {
		if t, ok := v.(T); ok {
			return t
		}
	}
	var t T
	if err := json.Unmarshal(b, &t); err != nil {
		return zero
	}
	mu.Lock()
	if len(cache) > 100000 {
		cache = map[string]any{}
	}
	cache[string(b)] = t
	mu.Unlock()
	return t
}

// Col returns column i's MySQL part.
func Col(d *core.TableDef, i int) Column {
	if i < 0 || i >= len(d.Columns) {
		return Column{}
	}
	return decode[Column](d.Columns[i].Engine)
}

// ColOf returns a column's MySQL part.
func ColOf(c core.ColumnDef) Column { return decode[Column](c.Engine) }

// Idx returns an index's MySQL part.
func Idx(ix *core.IndexDef) Index {
	if ix == nil {
		return Index{}
	}
	return decode[Index](ix.Engine)
}

// Tab returns a table's MySQL part.
func Tab(d *core.TableDef) Table { return decode[Table](d.Engine) }

// Src returns a table's Source mapping's MySQL part.
func Src(d *core.TableDef) Source {
	if d.Source == nil {
		return Source{}
	}
	return decode[Source](d.Source.Engine)
}

// LocalOf returns a local change's MySQL part.
func LocalOf(l core.LocalChange) Local { return decode[Local](l.Engine) }

// Obj returns an object's MySQL part.
func Obj(o core.Object) Object { return decode[Object](o.Engine) }

// NS is the core namespace of a MySQL database.
func NS(db string) []string { return []string{db} }

// DB is a table's MySQL database.
func DB(d *core.TableDef) string {
	if len(d.Namespace) == 0 {
		return ""
	}
	return d.Namespace[0]
}

// ObjDB is an object's MySQL database.
func ObjDB(o core.Object) string {
	if len(o.Namespace) == 0 {
		return ""
	}
	return o.Namespace[0]
}

// SourceDB is the Source database of a mapping.
func SourceDB(m *core.SourceMapping) string {
	if m == nil || len(m.Namespace) == 0 {
		return ""
	}
	return m.Namespace[0]
}

// ParentIndex finds the unique index of parent whose columns are a foreign
// key's referenced columns, by name.
func ParentIndex(parent *core.TableDef, fk ForeignKey) (core.IndexID, bool) {
	for _, ix := range parent.Indexes {
		if (!ix.Unique && !ix.Primary) || len(ix.Columns) != len(fk.ParentColumns) {
			continue
		}
		match := true
		for k, col := range ix.Columns {
			if int(col) >= len(parent.Columns) || !strings.EqualFold(parent.Columns[col].Name, fk.ParentColumns[k]) {
				match = false
				break
			}
		}
		if match {
			return ix.ID, true
		}
	}
	return 0, false
}

// KeyNeedsShadows reports whether a Source key can equal a Delta key without
// being byte-equal: when an identity column has a non-binary collation
// (product doc 03 §2.4).
func KeyNeedsShadows(d *core.TableDef) bool {
	cols := d.IdentityColumns()
	if cols == nil {
		cols = make([]uint16, len(d.Columns))
		for i := range cols {
			cols[i] = uint16(i)
		}
	}
	for _, c := range cols {
		coll := Col(d, int(c)).Collation
		if coll != "" && coll != "binary" && !strings.HasSuffix(coll, "_bin") {
			return true
		}
	}
	return false
}
