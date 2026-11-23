// Package source_test — tests/page_test.go
//
// Page continuation (doc 10 §4.1, D-84; Source scaling plan P1, P2): a page
// after delivered rows starts after the last row with an expanded keyset the
// Source can use as an index range, NULL-aware per column, and never with an
// OFFSET.
package source_test

import (
	"strings"
	"testing"

	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"

	core "github.com/virtual-db/vdb-core"

	. "github.com/virtual-db/vdb-mysql-engine/internal/source"
)

var intCol = mysqldef.Encode(mysqldef.Column{Type: "int"})

func pageTable(nullablePK bool) *core.TableDef {
	return &core.TableDef{ID: 1, Namespace: []string{"db"}, Name: "t", Kind: core.Mirrored,
		Columns:  []core.ColumnDef{{Name: "id", Nullable: nullablePK, Engine: intCol}, {Name: "v", Nullable: true, Engine: intCol}},
		Indexes:  []core.IndexDef{{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}}},
		Identity: 0, Source: &core.SourceMapping{Namespace: []string{"db"}, Table: "t"}}
}

// abTable: t(a NOT NULL, b NULL, id PK) with index ab(a, b).
func abTable() *core.TableDef {
	return &core.TableDef{ID: 1, Namespace: []string{"db"}, Name: "t", Kind: core.Mirrored,
		Columns: []core.ColumnDef{{Name: "a", Engine: intCol}, {Name: "b", Nullable: true, Engine: intCol}, {Name: "id", Engine: intCol}},
		Indexes: []core.IndexDef{{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{2}},
			{ID: 1, Name: "ab", Columns: []uint16{0, 1}}},
		Identity: 0, Source: &core.SourceMapping{Namespace: []string{"db"}, Table: "t"}}
}

func v(s string) core.Value { return core.Value{B: []byte(s)} }

var null = core.Value{Null: true}

func where(sql string) string {
	i, j := strings.Index(sql, " WHERE "), strings.Index(sql, " ORDER BY ")
	if i < 0 {
		return ""
	}
	return sql[i+7 : j]
}

func TestKeysetPage(t *testing.T) {
	cases := []struct {
		name  string
		d     *core.TableDef
		idx   core.IndexID
		after []core.Value
		incl  bool
		dir   core.Direction
		want  string
	}{
		{"one column", pageTable(false), 0, []core.Value{v("6")}, false, core.Forward,
			"(`id` > 6)"},
		{"expanded", abTable(), 1, []core.Value{v("5"), v("7"), v("9")}, false, core.Forward,
			"((`a` > 5) OR (`a` = 5 AND `b` > 7) OR (`a` = 5 AND `b` = 7 AND `id` > 9))"},
		{"backward, nullable b", abTable(), 1, []core.Value{v("5"), v("7"), v("9")}, false, core.Backward,
			"((`a` < 5) OR (`a` = 5 AND (`b` < 7 OR `b` IS NULL)) OR (`a` = 5 AND `b` = 7 AND `id` < 9))"},
		{"NULL first ascending", abTable(), 1, []core.Value{v("5"), null, v("9")}, false, core.Forward,
			"((`a` > 5) OR (`a` = 5 AND `b` IS NOT NULL) OR (`a` = 5 AND `b` IS NULL AND `id` > 9))"},
		{"NULL last descending", abTable(), 1, []core.Value{v("5"), null, v("9")}, false, core.Backward,
			"((`a` < 5) OR (`a` = 5 AND `b` IS NULL AND `id` < 9))"},
		{"inclusive", abTable(), 1, []core.Value{v("5"), v("7"), v("9")}, true, core.Forward,
			"((`a` > 5) OR (`a` = 5 AND `b` > 7) OR (`a` = 5 AND `b` = 7 AND `id` > 9) OR (`a` = 5 AND `b` = 7 AND `id` = 9))"},
	}
	for _, c := range cases {
		sql := PageSQL(c.d, core.ColumnSet{0, 1}, c.idx, c.after, c.incl, c.dir, 3)
		if strings.Contains(sql, "OFFSET") {
			t.Errorf("%s: a page never skips rows with an offset: %s", c.name, sql)
		}
		if got := where(sql); got != c.want {
			t.Errorf("%s:\n got  %s\n want %s", c.name, got, c.want)
		}
	}
}

func TestNullableKeyPagesByKeyset(t *testing.T) {
	sql := PageSQL(pageTable(true), core.ColumnSet{0, 1}, 0, []core.Value{v("6")}, false, core.Forward, 3)
	if strings.Contains(sql, "OFFSET") || where(sql) != "(`id` > 6)" {
		t.Fatalf("a nullable key column pages by keyset too: %s", sql)
	}
}
