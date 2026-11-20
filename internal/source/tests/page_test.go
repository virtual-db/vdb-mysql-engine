// Package source_test — tests/page_test.go
//
// Page continuation (doc 10 §4.1, D-84): a page after delivered rows starts
// after the last row when keyset continuation is safe, at an offset when it is
// not, and never both, which would skip the rows between them.
package source_test

import (
	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"
	"strings"
	"testing"

	core "github.com/virtual-db/vdb-core"

	. "github.com/virtual-db/vdb-mysql-engine/internal/source"
)

func pageTable(nullablePK bool) *core.TableDef {
	return &core.TableDef{ID: 1, Namespace: []string{"db"}, Name: "t", Kind: core.Mirrored,
		Columns:  []core.ColumnDef{{Name: "id", Nullable: nullablePK, Engine: mysqldef.Encode(mysqldef.Column{Type: "int"})}, {Name: "v", Nullable: true, Engine: mysqldef.Encode(mysqldef.Column{Type: "int"})}},
		Indexes:  []core.IndexDef{{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}}},
		Identity: 0, Source: &core.SourceMapping{Namespace: []string{"db"}, Table: "t"}}
}

func TestKeysetPageHasNoOffset(t *testing.T) {
	sql := PageSQL(pageTable(false), core.ColumnSet{0, 1}, 0, []core.Value{{B: []byte("6")}}, 3, 3)
	if strings.Contains(sql, "OFFSET") {
		t.Fatalf("a keyset page must not skip rows with an offset: %s", sql)
	}
	if !strings.Contains(sql, ">") {
		t.Fatalf("a keyset page starts after the last row: %s", sql)
	}
}

func TestOffsetPageWhenKeysetIsUnsafe(t *testing.T) {
	sql := PageSQL(pageTable(true), core.ColumnSet{0, 1}, 0, []core.Value{{B: []byte("6")}}, 3, 3)
	if !strings.Contains(sql, "OFFSET 3") {
		t.Fatalf("without a safe keyset the page starts at an offset: %s", sql)
	}
}
