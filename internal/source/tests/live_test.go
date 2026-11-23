// Package source_test — tests/live_test.go
//
// Pages against a real MySQL (Source scaling plan P1 to P4). Skipped unless
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
	"sort"
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
		got, sqls, err := WalkPages(q, Walk{Table: d, Columns: core.ColumnSet{0, 1, 2}, Index: 1, Dir: dir, Limit: 997})
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
			got, sqls, err := WalkPages(q, Walk{Table: d, Columns: core.ColumnSet{0, 1}, Index: core.WholeRow, Dir: dir, Limit: limit})
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

// handlerReads wraps query so each request records how many index entries
// the Source read for it.
func handlerReads(t *testing.T, db *sql.DB, reads *[]int) func(string) ([][]core.Value, error) {
	q := query(db)
	return func(s string) ([][]core.Value, error) {
		if _, err := db.Exec("FLUSH STATUS"); err != nil {
			return nil, err
		}
		rows, err := q(s)
		if err != nil {
			return nil, err
		}
		st, err := q("SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_key', 'Handler_read_next', 'Handler_read_prev', 'Handler_read_first', 'Handler_read_last', 'Handler_read_rnd_next')")
		if err != nil {
			return nil, err
		}
		n := 0
		for _, r := range st {
			var x int
			fmt.Sscan(string(r[1].B), &x)
			n += x
		}
		*reads = append(*reads, n)
		return rows, nil
	}
}

// TestLiveSparsePushedCondition (P4): a pushed condition few rows match is
// read one index chunk per request, never the rest of the index, and the
// walk returns exactly the matching rows in order.
func TestLiveSparsePushedCondition(t *testing.T) {
	db := liveDB(t)
	fillAB(t, db)
	d := liveTable(abTable())
	const cond = "`id` % 1000 = 0"
	for _, dir := range []core.Direction{core.Forward, core.Backward} {
		desc := ""
		if dir == core.Backward {
			desc = " DESC"
		}
		for _, idx := range []core.IndexID{0, 1} {
			order := "id" + desc
			if idx == 1 {
				order = "a" + desc + ", b" + desc + ", id" + desc
			}
			want, err := query(db)("SELECT a, b, id FROM vdb_page_test.t WHERE " + cond + " ORDER BY " + order)
			if err != nil {
				t.Fatal(err)
			}
			var reads []int
			got, sqls, err := WalkPages(handlerReads(t, db, &reads), Walk{Table: d, Columns: core.ColumnSet{0, 1, 2},
				Index: idx, Dir: dir, Limit: 100, Pushed: cond, Chunk: 1000})
			if err != nil {
				t.Fatal(err)
			}
			name := fmt.Sprintf("sparse idx %d dir %v", idx, dir)
			sameRows(t, name, got, want)
			most := 0
			for i, s := range sqls {
				if strings.Contains(s, "OFFSET") && !strings.Contains(s, "LIMIT 1 OFFSET") {
					t.Fatalf("%s: page with OFFSET: %s", name, s)
				}
				if reads[i] > most {
					most = reads[i]
				}
			}
			t.Logf("%s: %d requests, most index entries read by one: %d", name, len(sqls), most)
			if most > 2100 {
				t.Fatalf("%s: one request read %d index entries for chunks of 1000", name, most)
			}
			if len(sqls) < 30 {
				t.Fatalf("%s: %d requests; the walk was not chunked", name, len(sqls))
			}
		}
	}
}

// TestLiveByteCappedPages (P3): with a byte cap, pages shrink to about the
// cap and the walk still returns every row.
func TestLiveByteCappedPages(t *testing.T) {
	db := liveDB(t)
	fillAB(t, db)
	d := liveTable(abTable())
	q := query(db)
	want, err := q("SELECT a, b, id FROM vdb_page_test.t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	counting := func(s string) ([][]core.Value, error) {
		rows, err := q(s)
		sizes = append(sizes, len(rows))
		return rows, err
	}
	got, sqls, err := WalkPages(counting, Walk{Table: d, Columns: core.ColumnSet{0, 1, 2}, Index: 0, Dir: core.Forward,
		Limit: 5000, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	sameRows(t, "byte capped", got, want)
	// Rows are about 12 bytes here, so a 4 KiB page holds a few hundred.
	for i, n := range sizes[1:] {
		if n > 1000 {
			t.Fatalf("page %d held %d rows under a 4 KiB cap:\n%s", i+1, n, sqls[i+1])
		}
	}
	t.Logf("%d pages, first %d rows, second %d rows", len(sizes), sizes[0], sizes[1])
}

// TestLiveKeyPages (K2): a lookup of keys with 2,000 rows each is paged per
// key: every request stays near VDB_BATCH_ROWS rows, and each key's rows come
// back whole and in identity order, through NULL key parts too.
func TestLiveKeyPages(t *testing.T) {
	db := liveDB(t)
	fillAB(t, db)
	d := liveTable(abTable())
	q := query(db)
	keys := [][]core.Value{{v("3")}, {v("7")}, {v("3"), null}, {v("99")}, {v("5"), v("40")}}
	var want [][]core.Value
	for i, k := range keys {
		cond := "a = " + string(k[0].B)
		if len(k) > 1 {
			if k[1].Null {
				cond += " AND b IS NULL"
			} else {
				cond += " AND b = " + string(k[1].B)
			}
		}
		rows, err := q(fmt.Sprintf("SELECT %d, a, b, id FROM vdb_page_test.t WHERE %s ORDER BY id", i, cond))
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, rows...)
	}
	var sizes []int
	counting := func(s string) ([][]core.Value, error) {
		rows, err := q(s)
		sizes = append(sizes, len(rows))
		return rows, err
	}
	// Keys of two lengths go in separate requests; 2 keys per request.
	got, sqls, err := WalkKeys(counting, d, core.ColumnSet{0, 1, 2}, []uint16{0, 1}, keys, 2, 500)
	if err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(got, func(i, j int) bool { return string(got[i][0].B) < string(got[j][0].B) })
	sameRows(t, "key pages", got, want)
	for i, n := range sizes {
		if n > 500 {
			t.Fatalf("request %d returned %d rows for a budget of 500:\n%s", i, n, sqls[i])
		}
	}
	t.Logf("%d requests for %d rows", len(sqls), len(want))
}

// TestLiveKeyPagesKeyless (K2): keyless rows with many identical copies under
// one key page to exactly their rows.
func TestLiveKeyPagesKeyless(t *testing.T) {
	db := liveDB(t)
	exec(t, db, "CREATE TABLE k (x INT NULL, y INT NULL, KEY (x))")
	var vals []string
	for i := 0; i < 600; i++ {
		y := fmt.Sprint(i % 3)
		if i%13 == 0 {
			y = "NULL"
		}
		vals = append(vals, fmt.Sprintf("(%d, %s)", i%2, y))
	}
	exec(t, db, "INSERT INTO k VALUES "+strings.Join(vals, ","))
	d := liveTable(&core.TableDef{ID: 2, Name: "k", Kind: core.Mirrored,
		Columns:  []core.ColumnDef{{Name: "x", Nullable: true, Engine: intCol}, {Name: "y", Nullable: true, Engine: intCol}},
		Indexes:  []core.IndexDef{{ID: 0, Name: "x", Columns: []uint16{0}}},
		Identity: core.WholeRow})
	q := query(db)
	want, err := q("(SELECT 0, x, y FROM vdb_page_test.k WHERE x = 0 ORDER BY x, y) UNION ALL (SELECT 1, x, y FROM vdb_page_test.k WHERE x = 1 ORDER BY x, y) ORDER BY 1, 2, 3")
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []int{4, 7, 50, 10000} {
		got, _, err := WalkKeys(q, d, core.ColumnSet{0, 1}, []uint16{0}, [][]core.Value{{v("0")}, {v("1")}}, 2, rows)
		if err != nil {
			t.Fatal(err)
		}
		sort.SliceStable(got, func(i, j int) bool { return string(got[i][0].B) < string(got[j][0].B) })
		sameRows(t, fmt.Sprintf("keyless key pages, %d rows", rows), got, want)
	}
}
