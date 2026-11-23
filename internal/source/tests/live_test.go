// Package source_test — tests/live_test.go
//
// Pages against a real MySQL (Source scaling plan P1, P2). Skipped unless
// VDB_SOURCE_TEST_DSN names a MySQL 8.4 server the test may create the
// database vdb_page_test on, for example:
//
//	docker run -d --name my84 -e MYSQL_ROOT_PASSWORD=pw -p 3306:3306 mysql:8.4.13
//	VDB_SOURCE_TEST_DSN='root:pw@tcp(127.0.0.1:3306)/' go test ./internal/source/tests -run Live -v
package source_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	core "github.com/virtual-db/vdb-core"

	. "github.com/virtual-db/vdb-mysql-engine/internal/source"
)

func liveDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("VDB_SOURCE_TEST_DSN")
	if dsn == "" {
		t.Skip("VDB_SOURCE_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{"DROP DATABASE IF EXISTS vdb_page_test", "CREATE DATABASE vdb_page_test", "USE vdb_page_test"} {
		exec(t, db, q)
	}
	return db
}

func exec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func query(db *sql.DB) func(string) ([][]core.Value, error) {
	return func(q string) ([][]core.Value, error) {
		rs, err := db.Query(q)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", q, err)
		}
		defer rs.Close()
		cols, _ := rs.Columns()
		var out [][]core.Value
		for rs.Next() {
			raw := make([]sql.RawBytes, len(cols))
			ptr := make([]any, len(cols))
			for i := range raw {
				ptr[i] = &raw[i]
			}
			if err := rs.Scan(ptr...); err != nil {
				return nil, err
			}
			row := make([]core.Value, len(cols))
			for i, b := range raw {
				if b == nil {
					row[i] = core.Value{Null: true}
				} else {
					row[i] = core.Value{B: append([]byte(nil), b...)}
				}
			}
			out = append(out, row)
		}
		return out, rs.Err()
	}
}

func liveTable(d *core.TableDef) *core.TableDef {
	d.Namespace = []string{"vdb_page_test"}
	d.Source = &core.SourceMapping{Namespace: []string{"vdb_page_test"}, Table: d.Name}
	return d
}

func render(rows [][]core.Value) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		var parts []string
		for _, x := range r {
			if x.Null {
				parts = append(parts, "NULL")
			} else {
				parts = append(parts, string(x.B))
			}
		}
		out[i] = strings.Join(parts, ",")
	}
	return out
}

func sameRows(t *testing.T, name string, got, want [][]core.Value) {
	t.Helper()
	g, w := render(got), render(want)
	if len(g) != len(w) {
		t.Fatalf("%s: %d rows, want %d", name, len(g), len(w))
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("%s: row %d is %s, want %s", name, i, g[i], w[i])
		}
	}
}

// fillAB creates t(a NOT NULL, b NULL, id PK, KEY ab(a, b)): 20,000 rows,
// 2,000 per value of a, b NULL on every seventh row.
func fillAB(t *testing.T, db *sql.DB) {
	exec(t, db, "CREATE TABLE t (a INT NOT NULL, b INT NULL, id INT PRIMARY KEY, KEY ab (a, b))")
	for start := 0; start < 20000; start += 1000 {
		var vals []string
		for i := start; i < start+1000; i++ {
			b := fmt.Sprint(i % 100)
			if i%7 == 0 {
				b = "NULL"
			}
			vals = append(vals, fmt.Sprintf("(%d, %s, %d)", i%10, b, i))
		}
		exec(t, db, "INSERT INTO t VALUES "+strings.Join(vals, ","))
	}
	exec(t, db, "ANALYZE TABLE t")
}

// TestLivePageIsARange: a page after a row deep inside one value of a is an
// index range examining about one page of rows, not the rest of the value.
func TestLivePageIsARange(t *testing.T) {
	db := liveDB(t)
	fillAB(t, db)
	d := liveTable(abTable())
	q := query(db)
	group, err := q("SELECT a, b, id FROM vdb_page_test.t WHERE a = 5 ORDER BY a, b, id")
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{1000, 1 /* b IS NULL */} {
		after := group[at]
		sql := PageSQL(d, core.ColumnSet{0, 1, 2}, 1, after, false, core.Forward, 100)
		ex, err := q("EXPLAIN FORMAT=JSON " + sql)
		if err != nil {
			t.Fatal(err)
		}
		var plan struct {
			QueryBlock struct {
				Table struct {
					AccessType string  `json:"access_type"`
					Key        string  `json:"key"`
					Rows       float64 `json:"rows_examined_per_scan"`
				} `json:"table"`
				Ordering struct {
					Table struct {
						AccessType string  `json:"access_type"`
						Key        string  `json:"key"`
						Rows       float64 `json:"rows_examined_per_scan"`
					} `json:"table"`
				} `json:"ordering_operation"`
			} `json:"query_block"`
		}
		if err := json.Unmarshal(ex[0][0].B, &plan); err != nil {
			t.Fatal(err)
		}
		tb := plan.QueryBlock.Table
		if tb.AccessType == "" {
			tb = plan.QueryBlock.Ordering.Table
		}
		t.Logf("after %s: %s on %s\n%s", render([][]core.Value{after})[0], tb.AccessType, tb.Key, sql)
		if tb.AccessType != "range" || tb.Key != "ab" {
			t.Fatalf("page is %s on %q, want a range on ab:\n%s", tb.AccessType, tb.Key, ex[0][0].B)
		}
		// Rows the Source actually reads for the page: about the page itself.
		// The old row-constructor shape, (a, b, id) > (x, y, z), read every
		// row from a = x on (about 1,000 here from row 1000, 2,000 from 1).
		exec(t, db, "FLUSH STATUS")
		got, err := q(sql)
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, "page", got, group[at+1:at+101])
		st, err := q("SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_key', 'Handler_read_next', 'Handler_read_prev', 'Handler_read_first', 'Handler_read_last')")
		if err != nil {
			t.Fatal(err)
		}
		read := 0
		for _, r := range st {
			var n int
			fmt.Sscan(string(r[1].B), &n)
			read += n
		}
		t.Logf("handler reads for the page: %d", read)
		if read > 300 {
			t.Fatalf("the page read %d index entries for 100 rows", read)
		}
	}
}

// TestLiveWalk: every page walk returns exactly the rows of one ORDER BY,
// forward and backward, through NULLs.
func TestLiveWalk(t *testing.T) {
	db := liveDB(t)
	fillAB(t, db)
	d := liveTable(abTable())
	q := query(db)
	for _, dir := range []core.Direction{core.Forward, core.Backward} {
		desc := ""
		if dir == core.Backward {
			desc = " DESC"
		}
		want, err := q("SELECT a, b, id FROM vdb_page_test.t ORDER BY a" + desc + ", b" + desc + ", id" + desc)
		if err != nil {
			t.Fatal(err)
		}
		got, sqls, err := WalkPages(q, d, core.ColumnSet{0, 1, 2}, 1, dir, 997)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sqls {
			if strings.Contains(s, "OFFSET") {
				t.Fatalf("page with OFFSET: %s", s)
			}
		}
		sameRows(t, fmt.Sprint("walk ", dir), got, want)
	}
}

// TestLiveKeylessDuplicates: a keyless table with many identical rows, some
// with NULLs, pages to exactly its rows, even when one value has more
// identical rows than a page holds.
func TestLiveKeylessDuplicates(t *testing.T) {
	db := liveDB(t)
	exec(t, db, "CREATE TABLE k (x INT NULL, y INT NULL)")
	var vals []string
	for i := 0; i < 600; i++ {
		x, y := fmt.Sprint(i%5), fmt.Sprint(i%3)
		if i%11 == 0 {
			x = "NULL"
		}
		if i%13 == 0 {
			y = "NULL"
		}
		if i < 40 {
			x, y = "9", "9" // 40 identical rows, more than one page
		}
		vals = append(vals, "("+x+", "+y+")")
	}
	exec(t, db, "INSERT INTO k VALUES "+strings.Join(vals, ","))
	d := liveTable(&core.TableDef{ID: 2, Name: "k", Kind: core.Mirrored,
		Columns:  []core.ColumnDef{{Name: "x", Nullable: true, Engine: intCol}, {Name: "y", Nullable: true, Engine: intCol}},
		Identity: core.WholeRow})
	q := query(db)
	for _, dir := range []core.Direction{core.Forward, core.Backward} {
		desc := ""
		if dir == core.Backward {
			desc = " DESC"
		}
		want, err := q("SELECT x, y FROM vdb_page_test.k ORDER BY x" + desc + ", y" + desc)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{7, 25, 1000} {
			got, sqls, err := WalkPages(q, d, core.ColumnSet{0, 1}, core.WholeRow, dir, limit)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range sqls {
				if strings.Contains(s, "OFFSET") {
					t.Fatalf("page with OFFSET: %s", s)
				}
			}
			sameRows(t, fmt.Sprintf("keyless dir %v limit %d", dir, limit), got, want)
		}
	}
}
