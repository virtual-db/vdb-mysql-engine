// Package source — snapshot.go
//
// The Source half of a snapshot pair (doc 10 §2): a pinned connection inside
// START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY, its mirrored
// session settings, its request cache, and every read the core asks of it.
package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"
	"io"
	"strconv"
	"sync"
	"time"

	core "github.com/virtual-db/vdb-core"
)

// MirroredSettings are the session settings mirrored to the Source (product doc 08 §4).
var MirroredSettings = []string{"time_zone", "sql_mode", "div_precision_increment", "collation_connection", "character_set_connection"}

type snapshot struct {
	a        *Adapter
	info     core.SessionInfo
	ctx      core.StmtContext
	digest   string
	conn     *sql.Conn
	off      bool // VDB_SOURCE_SNAPSHOT=off: no transaction, any pooled connection
	mu       sync.Mutex
	opened   time.Time
	lastUsed time.Time
	mirrored map[string]string
	expired  bool
	checked  uint64 // the statement whose first request checked the age; 0: none yet
	closed   bool
	noPush   bool
	rows     int64
}

func (s *snapshot) open() error {
	if err := s.reserve(); err != nil {
		return err
	}
	return s.begin()
}

// reserve takes a pooled connection and mirrors the session settings onto
// it; it may wait up to the connect wait for a free connection.
func (s *snapshot) reserve() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.a.cfg.ConnectWait)
	defer cancel()
	conn, err := s.a.snapPool.Conn(ctx)
	if err != nil {
		return &core.Error{Code: core.ErrSource, Msg: "Source connection limit reached or Source unavailable: " + err.Error() + "; retry the statement"}
	}
	s.conn = conn
	if err := s.mirror(); err != nil {
		conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

// Start opens the consistent snapshot on the reserved connection.
func (s *snapshot) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.off {
		return nil
	}
	s.opened, s.lastUsed = time.Now(), time.Now()
	if err := s.begin(); err != nil {
		return err
	}
	s.a.mu.Lock()
	s.a.snapshots[s] = true
	s.a.mu.Unlock()
	return nil
}

func (s *snapshot) begin() error {
	conn := s.conn
	if _, err := conn.ExecContext(context.Background(), s.a.comment(s.info.ConnID)+"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		conn.Close()
		s.conn = nil
		return &core.Error{Code: core.ErrSource, Msg: "cannot open a Source snapshot: " + err.Error()}
	}
	return nil
}

// mirror applies the evaluation context's settings that differ from what the
// connection already has. A setting the Source refuses turns pushdown off for
// the session, with a warning.
func (s *snapshot) mirror() error {
	var set [][2]string
	for _, name := range MirroredSettings {
		v, ok := s.ctx.Eval[name]
		if !ok || s.mirrored[name] == v {
			continue
		}
		set = append(set, [2]string{name, v})
	}
	if len(set) == 0 {
		return nil
	}
	q := querier(s.a.snapPool)
	if s.conn != nil {
		q = s.conn
	}
	sh := renderSet(set)
	if err := check(sh); err != nil {
		return err
	}
	if _, err := q.ExecContext(context.Background(), s.a.comment(s.info.ConnID)+sh.sql); err != nil {
		log.Warn("Source refused a mirrored setting; pushdown is off for this session", "conn", s.info.ConnID, "err", err)
		s.noPush = true
		return nil
	}
	for _, kv := range set {
		s.mirrored[kv[0]] = kv[1]
	}
	return nil
}

func (s *snapshot) idleFor() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastUsed)
}

// expire closes an idle snapshot; its next request fails or reopens.
func (s *snapshot) expire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.expired {
		return
	}
	s.expired = true
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.a.forget(s)
	if s.a.emit != nil {
		s.a.emit("vdb.snapshot.expired", map[string]any{"ConnectionID": s.info.ConnID, "AgeMs": time.Since(s.opened).Milliseconds()})
	}
}

// before checks the snapshot's limits before a request. The maximum age is
// checked only on a statement's first Source request (SD-2): a statement
// that is still reading keeps its snapshot however long it runs, and an open
// transaction older than the maximum age fails at its next statement. An
// idle snapshot (closed by the watchdog) fails or renews on any request.
func (s *snapshot) before() (querier, error) {
	stmt := s.a.statement(s.info.ConnID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("source: snapshot closed")
	}
	if s.off {
		s.lastUsed = time.Now()
		return s.a.snapPool, nil
	}
	tooOld := AgeExpired(stmt, &s.checked, s.opened, time.Now(), s.a.cfg.SnapshotMaxAge)
	if s.expired || tooOld {
		if s.a.cfg.SnapshotPolicy != "relaxed" {
			if tooOld {
				return nil, ErrTooOld(s.a.cfg.SnapshotMaxAge)
			}
			return nil, &core.Error{Code: core.ErrSource, Msg: "the Source snapshot expired after VDB_SOURCE_SNAPSHOT_IDLE_TIMEOUT idle; retry the transaction"}
		}
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		s.mirrored = map[string]string{}
		if err := s.open(); err != nil {
			return nil, err
		}
		s.opened, s.expired = time.Now(), false
		log.Warn("Source snapshot renewed under the relaxed policy", "conn", s.info.ConnID)
	}
	s.lastUsed = time.Now()
	return s.conn, nil
}

// AgeExpired reports whether a snapshot opened at opened is past maxAge at a
// request of statement stmt (the connection's statement count). Only the
// first request of a statement is checked (SD-2); checked holds the last
// statement checked. stmt 0 means statements are not counted: every request
// is checked.
func AgeExpired(stmt uint64, checked *uint64, opened, now time.Time, maxAge time.Duration) bool {
	first := stmt == 0 || stmt != *checked
	*checked = stmt
	return first && now.Sub(opened) > maxAge
}

// ErrTooOld is the strict policy's error for a snapshot past its maximum age.
func ErrTooOld(maxAge time.Duration) error {
	return &core.Error{Code: core.ErrSource, Msg: fmt.Sprintf("the Source snapshot is older than VDB_SOURCE_SNAPSHOT_MAX_AGE (%s); retry the transaction", maxAge)}
}

func (s *snapshot) run(d *core.TableDef, sh shaped) ([][]core.Value, error) {
	q, err := s.before()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.a.cfg.RequestTimeout+5*time.Second)
	defer cancel()
	rows, err := s.a.run(ctx, q, s.info.ConnID, s.digest, d, sh)
	if err != nil {
		return nil, &core.Error{Code: core.ErrSource, Table: d.ID, Msg: "Source request failed for " + mysqldef.DB(d) + "." + d.Name + ": " + err.Error()}
	}
	s.rows += int64(len(rows))
	if max := s.a.cfg.MaxSourceRows; max > 0 && s.rows > max {
		return nil, &core.Error{Code: core.ErrSource, Table: d.ID, Msg: fmt.Sprintf("the statement read more than VDB_MAX_SOURCE_ROWS (%d) rows from the Source", max)}
	}
	return rows, nil
}

// fill turns result rows of a projection into requested rows and keys.
func fill(d *core.TableDef, p projection, raw [][]core.Value, skip int) core.SourceRows {
	var out core.SourceRows
	for _, r := range raw {
		r = r[skip:]
		row := make(core.Row, len(p.reqAt))
		for i, at := range p.reqAt {
			if at >= 0 && at < len(r) {
				row[i] = r[at]
				continue
			}
			c := d.Columns[p.reqCols[i]]
			if c.Fetch == core.Default {
				row[i] = c.Default
			} else {
				row[i] = core.Value{Null: true}
			}
		}
		kv := make([]core.Value, len(p.keyAt))
		for i, at := range p.keyAt {
			kv[i] = r[at]
		}
		out.Rows = append(out.Rows, row)
		out.Keys = append(out.Keys, core.EncodeSourceKey(kv))
	}
	return out
}

func (s *snapshot) pushed(d *core.TableDef, cond string) (string, bool) {
	if cond == "" || s.noPush || !s.a.cfg.Pushdown || s.a.pushdown == nil {
		return "", false
	}
	return s.a.pushdown.Reduce(d, cond)
}

// Scan opens a paged Source cursor (D-84).
func (s *snapshot) Scan(req core.SourceScan) (core.SourceCursor, error) {
	d, err := s.a.table(req.Table)
	if err != nil {
		return nil, err
	}
	pushed, none := s.pushed(d, req.Pushed)
	idx := req.Index
	if !req.Ordered {
		idx = d.Identity
	}
	order := pageOrder(d, idx)
	c := &pager{s: s, d: d, p: project(d, req.Columns), req: req, idx: idx, pushed: pushed, none: none,
		order: order, keyless: d.Identity == core.WholeRow, limit: s.a.cfg.BatchRows, maxBytes: s.a.cfg.BatchBytes,
		chunk: s.a.cfg.BatchRows}
	if s.a.rowLen != nil {
		c.rowBytes = int(s.a.rowLen(d))
	}
	return c, nil
}

// pager reads one page per Next; each page is read to its end before the
// first row is returned, so the connection is idle between pages. Every page
// after the first starts after the last row delivered (keyset), never at an
// offset. A keyless table pages on all its columns: identical rows are not
// told apart by any column, so its pages start at the last row delivered
// (inclusive) and skip the identical rows already delivered (dup).
type pager struct {
	s       *snapshot
	d       *core.TableDef
	p       projection
	req     core.SourceScan
	idx     core.IndexID
	pushed  string
	none    bool
	order   []uint16
	keyless bool
	after   []core.Value
	dup     int // rows identical to after already delivered (keyless)
	limit   int
	done    bool

	// P3: a page's rows are also capped by maxBytes over the estimated row
	// size: the Source's average row length at first, then the last page's.
	maxBytes int
	rowBytes int

	// P4: with a pushed condition, a page also stops at the chunk bound, the
	// row chunk rows past after on the index, so no request examines more
	// than about one chunk however few rows the condition matches.
	chunk   int
	bounded bool         // the current chunk's bound has been read
	bound   []core.Value // nil: the current chunk runs to the end
}

// pageLimit is the next page's row limit: the smaller of VDB_BATCH_ROWS (or
// what the caller asked) and VDB_BATCH_BYTES over the estimated row size.
func (c *pager) pageLimit(maxRows int) int {
	limit := c.limit
	if maxRows > 0 && maxRows < limit {
		limit = maxRows
	}
	if c.maxBytes > 0 && c.rowBytes > 0 {
		if n := c.maxBytes / c.rowBytes; n < limit {
			limit = n
		}
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// observe updates the row size estimate from a page: rows and their bytes.
func (c *pager) observe(rows [][]core.Value) {
	if len(rows) == 0 {
		return
	}
	n := 0
	for _, r := range rows {
		for _, v := range r {
			n += len(v.B) + 1
		}
	}
	c.rowBytes = (n + len(rows) - 1) / len(rows)
}

// chunked reports whether pages of this scan stop at chunk bounds: those
// with a pushed condition on a table with a key. A keyless table's rows can
// be identical across a bound, so it keeps single requests.
func (c *pager) chunked() bool { return c.pushed != "" && !c.keyless && c.chunk > 0 }

func (c *pager) Next(maxRows int) (core.SourceRows, error) {
	raw, err := c.next(maxRows, func(sh shaped) ([][]core.Value, error) { return c.s.run(c.d, sh) })
	out := fill(c.d, c.p, raw, 0)
	if err == io.EOF && len(out.Rows) > 0 {
		return out, nil
	}
	return out, err
}

// next reads the next page through run and returns its rows (the
// projection only); io.EOF once the scan is done and nothing is left.
func (c *pager) next(maxRows int, run func(shaped) ([][]core.Value, error)) ([][]core.Value, error) {
	if c.done || c.none {
		return nil, io.EOF
	}
	for {
		limit := c.pageLimit(maxRows)
		if c.chunked() && !c.bounded {
			b, err := run(renderBound(c.d, c.idx, c.req.Ranges, c.after, c.req.Dir, c.chunk, c.req.Ordered))
			if err != nil {
				return nil, err
			}
			c.bound, c.bounded = nil, true
			if len(b) > 0 {
				c.bound = b[0]
			}
		}
		sh, base, skip := c.page(limit)
		raw, err := run(sh)
		if err != nil {
			return nil, err
		}
		trimmed, done := c.take(raw, base, limit, skip)
		c.observe(trimmed)
		if done && c.bound != nil {
			// The chunk is read; the scan goes on after its bound.
			c.after, c.bound, c.bounded, done = c.bound, nil, false, false
		}
		c.done = done
		if len(trimmed) == 0 && c.done {
			return nil, io.EOF
		}
		if len(trimmed) > 0 || c.done {
			return trimmed, nil
		}
		// An empty chunk: the next request reads the next one.
	}
}

// page renders the next page: the projection, then the order columns for
// keyset continuation (from position base). skip is how many identical rows
// of a keyless table the page repeats at its start.
func (c *pager) page(limit int) (sh shaped, base, skip int) {
	p := c.p
	base = len(p.exprs)
	p.exprs = append(append([]string(nil), p.exprs...), rawCols(c.d, c.order)...)
	incl := c.keyless && c.after != nil
	if incl {
		skip = c.dup
	}
	return renderPage(c.d, p, c.idx, c.req.Ranges, c.after, incl, c.bound, c.pushed, c.req.Dir, limit+skip, c.req.Ordered), base, skip
}

// take records where the page ended and returns its new rows without the
// order columns; done when the page came back short.
func (c *pager) take(raw [][]core.Value, base, limit, skip int) ([][]core.Value, bool) {
	done := len(raw) < limit+skip
	// Drop the identical rows a keyless page repeats from the last one.
	for skip > 0 && len(raw) > 0 && equalVals(raw[0][base:], c.after) {
		raw = raw[1:]
		skip--
	}
	if len(raw) > 0 {
		last := raw[len(raw)-1][base:]
		if c.keyless {
			n := 0
			for i := len(raw) - 1; i >= 0 && equalVals(raw[i][base:], last); i-- {
				n++
			}
			if n == len(raw) && c.after != nil && equalVals(c.after, last) {
				n += c.dup
			}
			c.dup = n
		}
		c.after = append([]core.Value(nil), last...)
	}
	trimmed := make([][]core.Value, len(raw))
	for i, r := range raw {
		trimmed[i] = r[:base]
	}
	return trimmed, done
}

// Walk describes a scan for WalkPages.
type Walk struct {
	Table    *core.TableDef
	Columns  core.ColumnSet
	Index    core.IndexID
	Dir      core.Direction
	Limit    int    // VDB_BATCH_ROWS
	MaxBytes int    // VDB_BATCH_BYTES (0: none)
	Pushed   string // a pushed condition, already reduced
	Chunk    int    // the chunk size of P4 (0: VDB_BATCH_ROWS)
}

// WalkPages reads a whole scan through query, request by request, exactly
// as a scan's pager would, and returns the rows, every SQL statement sent and
// each page's row count; for tests against a real Source.
func WalkPages(query func(sql string) ([][]core.Value, error), w Walk) ([][]core.Value, []string, error) {
	d := w.Table
	chunk := w.Chunk
	if chunk == 0 {
		chunk = w.Limit
	}
	c := &pager{d: d, p: project(d, w.Columns), req: core.SourceScan{Table: d.ID, Index: w.Index, Dir: w.Dir, Ordered: true, Columns: w.Columns},
		idx: w.Index, order: pageOrder(d, w.Index), keyless: d.Identity == core.WholeRow, limit: w.Limit,
		maxBytes: w.MaxBytes, pushed: w.Pushed, chunk: chunk}
	var rows [][]core.Value
	var sqls []string
	run := func(sh shaped) ([][]core.Value, error) {
		sqls = append(sqls, sh.sql)
		if len(sqls) > 100000 {
			return nil, fmt.Errorf("WalkPages: no progress")
		}
		return query(sh.sql)
	}
	for {
		out, err := c.next(0, run)
		if err == io.EOF {
			return rows, sqls, nil
		}
		if err != nil {
			return rows, sqls, err
		}
		rows = append(rows, out...)
	}
}

func (c *pager) Close() error { c.done = true; return nil }

// PageSQL renders the SQL of the page a scan reads after the row after
// (inclusive with incl), in direction dir; for tests of continuation.
func PageSQL(d *core.TableDef, cols core.ColumnSet, idx core.IndexID, after []core.Value, incl bool, dir core.Direction, limit int) string {
	return renderPage(d, project(d, cols), idx, nil, after, incl, nil, "", dir, limit, true).sql
}

// Lookup answers keys on an index as a cursor: vdb_lookup_batch keys per
// request, each key's rows paged (K2).
func (s *snapshot) Lookup(t core.TableID, idx core.IndexID, keys []core.Key, cols core.ColumnSet) (core.SourceCursor, error) {
	d, err := s.a.table(t)
	if err != nil {
		return nil, err
	}
	ix := d.Index(idx)
	var icols []uint16
	if ix != nil {
		icols = ix.Columns
	} else {
		icols = identityCols(d)
	}
	vals := make([][]core.Value, len(keys))
	for i, k := range keys {
		v := core.DecodeSourceKey(k.Raw)
		if len(v) > len(icols) {
			v = v[:len(icols)]
		}
		vals[i] = v
	}
	return s.keys(d, cols, icols, vals, ShapeKeyBatch, false), nil
}

// keys returns the cursor over the rows matching vals on keyCols.
func (s *snapshot) keys(d *core.TableDef, cols core.ColumnSet, keyCols []uint16, vals [][]core.Value, shape string, first bool) *keyCursor {
	return newKeyCursor(d, cols, keyCols, vals, shape, first, s.a.cfg.LookupBatch, s.a.cfg.BatchRows,
		func(sh shaped) ([][]core.Value, error) { return s.run(d, sh) })
}

// keyCursor reads the rows of a list of keys (Source scaling plan K2). Each
// request is one tagged UNION ALL over up to batch keys; each key's part is
// ordered by the identity and limited, so a key with many rows never comes
// back whole. A key whose part comes back full is asked again in the next
// request, after its last row. With first, one row per key is enough
// (existence checks), and no key is asked twice.
type keyCursor struct {
	d       *core.TableDef
	p       projection
	keyCols []uint16
	vals    [][]core.Value
	shape   string
	first   bool
	batch   int // keys per request (vdb_lookup_batch)
	rows    int // rows per request (VDB_BATCH_ROWS)
	run     func(shaped) ([][]core.Value, error)

	order   []uint16
	keyless bool
	queue   []int                // keys still to read; continued keys first
	after   map[int][]core.Value // a continued key's last row, in order columns
	dup     map[int]int          // keyless: identical rows already returned at after
}

func newKeyCursor(d *core.TableDef, cols core.ColumnSet, keyCols []uint16, vals [][]core.Value, shape string, first bool,
	batch, rows int, run func(shaped) ([][]core.Value, error)) *keyCursor {
	c := &keyCursor{d: d, p: project(d, cols), keyCols: keyCols, vals: vals, shape: shape, first: first,
		batch: batch, rows: rows, run: run, order: identityCols(d), keyless: d.Identity == core.WholeRow,
		after: map[int][]core.Value{}, dup: map[int]int{}}
	if c.batch < 1 {
		c.batch = 1
	}
	for i, v := range vals {
		if len(v) > 0 {
			c.queue = append(c.queue, i)
		}
	}
	return c
}

// Next reads one request: the rows of the next keys, grouped by key, each
// with the index of its key in KeyIdx.
func (c *keyCursor) Next(maxRows int) (core.SourceRows, error) {
	for len(c.queue) > 0 {
		n := c.batch
		if n > len(c.queue) {
			n = len(c.queue)
		}
		// One request holds keys of one length (a prefix lookup's keys all
		// have the same number of parts).
		width := len(c.vals[c.queue[0]])
		part := make([]int, 0, n)
		var rest []int
		for _, k := range c.queue {
			if len(part) < n && len(c.vals[k]) == width {
				part = append(part, k)
			} else {
				rest = append(rest, k)
			}
		}
		budget := c.rows
		if maxRows > 0 && maxRows < budget {
			budget = maxRows
		}
		limit := budget / len(part)
		if limit < 2 {
			limit = 2 // a unique key's one row never looks full
		}
		if c.first {
			limit = 1
		}
		p := c.p
		base := len(p.exprs)
		p.exprs = append(append([]string(nil), p.exprs...), rawCols(c.d, c.order)...)
		pages := make([]keyPage, len(part))
		for i, k := range part {
			pages[i] = keyPage{vals: c.vals[k], after: c.after[k], incl: c.keyless && c.after[k] != nil, limit: limit + c.dup[k]}
		}
		raw, err := c.run(renderKeyPages(c.d, p, c.keyCols[:width], c.order, pages, c.shape))
		if err != nil {
			return core.SourceRows{}, err
		}
		var again []int
		var out core.SourceRows
		at := 0
		for i, k := range part {
			j := at
			for j < len(raw) && tagOf(raw[j]) == i {
				j++
			}
			got := raw[at:j]
			at = j
			full := len(got) == pages[i].limit && !c.first
			// Drop the identical rows a keyless page repeats at its start.
			for skip := c.dup[k]; skip > 0 && len(got) > 0 && equalVals(got[0][1+base:], c.after[k]); skip-- {
				got = got[1:]
			}
			if full && len(got) > 0 {
				last := got[len(got)-1][1+base:]
				if c.keyless {
					same := 0
					for x := len(got) - 1; x >= 0 && equalVals(got[x][1+base:], last); x-- {
						same++
					}
					if same == len(got) && equalVals(c.after[k], last) {
						same += c.dup[k]
					}
					c.dup[k] = same
				}
				c.after[k] = append([]core.Value(nil), last...)
				again = append(again, k)
			} else {
				delete(c.after, k)
				delete(c.dup, k)
			}
			trimmed := make([][]core.Value, len(got))
			for x, r := range got {
				trimmed[x] = r[:1+base]
			}
			f := fill(c.d, c.p, trimmed, 1)
			out.Rows = append(out.Rows, f.Rows...)
			out.Keys = append(out.Keys, f.Keys...)
			for range f.Rows {
				out.KeyIdx = append(out.KeyIdx, uint32(k))
			}
		}
		c.queue = append(again, rest...)
		if len(out.Rows) > 0 || len(c.queue) == 0 {
			return out, nil
		}
	}
	return core.SourceRows{}, io.EOF
}

func (c *keyCursor) Close() error { c.queue = nil; return nil }

func tagOf(r []core.Value) int {
	n, _ := strconv.Atoi(string(r[0].B))
	return n
}

// WalkKeys reads the rows of keys on keyCols through query, request by
// request, exactly as a lookup would: it returns the rows (with the key index
// first), every SQL statement sent; for tests against a real Source.
func WalkKeys(query func(sql string) ([][]core.Value, error), d *core.TableDef, cols core.ColumnSet, keyCols []uint16,
	keys [][]core.Value, batch, rows int) ([][]core.Value, []string, error) {
	var sqls []string
	c := newKeyCursor(d, cols, keyCols, keys, ShapeKeyBatch, false, batch, rows, func(sh shaped) ([][]core.Value, error) {
		sqls = append(sqls, sh.sql)
		if len(sqls) > 100000 {
			return nil, fmt.Errorf("WalkKeys: no progress")
		}
		return query(sh.sql)
	})
	var out [][]core.Value
	for {
		sr, err := c.Next(0)
		if err == io.EOF {
			return out, sqls, nil
		}
		if err != nil {
			return out, sqls, err
		}
		for i, r := range sr.Rows {
			out = append(out, append([]core.Value{{B: []byte(strconv.Itoa(int(sr.KeyIdx[i])))}}, r...))
		}
	}
}

// Exists reports which identity keys exist on the Source.
func (s *snapshot) Exists(t core.TableID, keys []core.SourceKey) ([]bool, error) {
	d, err := s.a.table(t)
	if err != nil {
		return nil, err
	}
	vals := make([][]core.Value, len(keys))
	for i, k := range keys {
		vals[i] = core.DecodeSourceKey(k)
	}
	icols := identityCols(d)
	out := make([]bool, len(keys))
	// For tables without a usable key the "key" is the whole row; ask by every column.
	c := s.keys(d, core.ColumnSet{}, icols, vals, ShapeExists, true)
	for {
		sr, err := c.Next(0)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		for _, k := range sr.KeyIdx {
			out[k] = true
		}
	}
}

// Count runs SELECT COUNT(*).
func (s *snapshot) Count(t core.TableID) (uint64, error) {
	d, err := s.a.table(t)
	if err != nil {
		return 0, err
	}
	raw, err := s.run(d, renderCount(d))
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(string(raw[0][0].B), 10, 64)
}

// ByColumns returns the rows whose columns cols equal one of values, as a
// cursor paged per value like a lookup (K3).
func (s *snapshot) ByColumns(t core.TableID, cols core.ColumnSet, values []core.Row, want core.ColumnSet) (core.SourceCursor, error) {
	d, err := s.a.table(t)
	if err != nil {
		return nil, err
	}
	vals := make([][]core.Value, len(values))
	for i, v := range values {
		vals[i] = v
	}
	return s.keys(d, want, cols, vals, ShapeColumns, false), nil
}

// Close commits the read-only transaction and returns the connection.
func (s *snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.a.forget(s)
	if s.conn != nil {
		sh := shaped{sql: "COMMIT", shape: ShapeSession}
		_, _ = s.conn.ExecContext(context.Background(), s.a.comment(s.info.ConnID)+sh.sql)
		s.conn.Close()
		s.conn = nil
	}
	return nil
}
