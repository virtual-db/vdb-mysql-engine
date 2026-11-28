// Package engine — writes.go
//
// InnoDB's rules around each read and write the core performs (plan doc 09
// §3.2, §3.3): the locks a locking read, an insert, an update and a delete
// take; foreign-key checks and cascades; statement rollback at READ
// COMMITTED. The core stores and merges; this file decides what to lock.
package engine

import (
	"bytes"

	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"

	core "github.com/virtual-db/vdb-core"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/virtual-db/vdb-mysql-engine/internal/fk"
	"github.com/virtual-db/vdb-mysql-engine/internal/innodb"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

func (c *conn) lock(s core.Session, r core.Resource, mode core.LockMode) error {
	return c.h.api.Locks().Acquire(s, r, mode, c.onWait)
}

// readLocks returns the hooks of a locking read in protocol mode m through
// index idx of table t, reading cols, or nil for a plain read. A unique lookup
// that finds nothing takes a gap lock at REPEATABLE READ and SERIALIZABLE
// (D-64).
//
// inInterval says every row the read returns lies inside an interval lock
// this transaction took for the read: a locking scan at REPEATABLE READ and
// SERIALIZABLE (intervalLocks). Such a row takes no record lock of its own
// (plan 03 L1) unless InnoDB would lock its clustered record too
// (locksRecord); otherwise it only waits for other transactions' record locks
// on it, and its interval blocks later writers through their probes
// (probeEntries). Every row also probes other transactions' intervals on its
// entry in idx, the only index the read goes through.
func (c *conn) readLocks(s core.Session, m protocol.LockMode, t core.TableID, idx core.IndexID, cols core.ColumnSet, inInterval bool) *core.ReadLocks {
	mode, ok := innodb.Record(m)
	if !ok {
		return nil
	}
	rowMode := mode
	if inInterval && !c.locksRecord(t, idx, cols, mode) {
		rowMode = innodb.Wait(mode)
	}
	rl := &core.ReadLocks{
		Row: func(t core.TableID, key core.KeyImage) error {
			created, err := c.h.api.Locks().AcquireNew(s, core.Resource{Table: t, Point: key}, rowMode, c.onWait)
			if err != nil {
				return err
			}
			c.noteLock(key, m, created && rowMode == mode)
			return nil
		},
		Probe: func(t core.TableID, row core.Row, cols core.ColumnSet) error {
			return c.probeEntries(s, t, row, cols, mode, only(idx))
		},
	}
	if c.iso.Gaps() {
		rl.Missing = func(t core.TableID, idx core.IndexID, img core.KeyImage) error {
			return c.lock(s, innodb.Span(t, idx, core.ImageRange{Low: img, High: img, LowIncl: true, HighIncl: true}), innodb.Gap)
		}
	}
	return rl
}

// locksRecord reports whether InnoDB locks the clustered record of each row a
// locking read through index idx returns, besides the index entry: when idx
// is a secondary index and the read is exclusive or needs a column that the
// index entry (its columns plus the identity key's) does not hold. A read
// through the identity key locks only that key's entries, which are the
// clustered records. cols nil means every column.
func (c *conn) locksRecord(t core.TableID, idx core.IndexID, cols core.ColumnSet, mode core.LockMode) bool {
	d, ok := c.h.api.Catalog().Table(t)
	if !ok {
		return true
	}
	ix := d.Index(idx)
	if ix == nil || (d.Identity != core.WholeRow && idx == d.Identity) {
		return false
	}
	if mode == innodb.RecordX {
		return true
	}
	have := make([]bool, len(d.Columns))
	for _, col := range ix.Columns {
		if int(col) < len(have) {
			have[col] = true
		}
	}
	for _, col := range d.IdentityColumns() {
		if int(col) < len(have) {
			have[col] = true
		}
	}
	if cols == nil {
		cols = make(core.ColumnSet, len(d.Columns))
		for i := range cols {
			cols[i] = uint16(i)
		}
	}
	return !covered(have, cols)
}

// only selects index idx for probeEntries.
func only(idx core.IndexID) func(core.IndexID) bool {
	return func(i core.IndexID) bool { return i == idx }
}

// changed selects, for probeEntries, the indexes an update from oldRow to row
// writes: the identity key, whose entry is the clustered record, and every
// index with a column whose value changes.
func changed(d *core.TableDef, oldRow, row core.Row) func(core.IndexID) bool {
	return func(i core.IndexID) bool {
		if d.Identity != core.WholeRow && i == d.Identity {
			return true
		}
		ix := d.Index(i)
		return ix == nil || !sameCols(oldRow, row, ix.Columns)
	}
}

// probeEntries makes a record lock in mode on row wait for the interval
// locks other transactions hold over the row's entries in the indexes want
// selects, or every index when want is nil (vdb-mysql-engine#6): a row record
// lock lives in its own key space, so without this probe a held range would
// not block a writer that reaches one of its rows by another path. As on
// InnoDB, a locking read probes only the index it reads through and a writer
// every index entry it writes. It does nothing when no other transaction
// holds an interval on the table. row holds the values of cols, or every
// column when cols is nil. An index whose columns the read did not fetch is
// probed whole: the record lock then waits for any interval on that index,
// which is stricter than InnoDB, never looser.
func (c *conn) probeEntries(s core.Session, t core.TableID, row core.Row, cols core.ColumnSet, mode core.LockMode, want func(core.IndexID) bool) error {
	idxs := c.h.api.Locks().RangeIndexes(s, t)
	if want != nil {
		kept := idxs[:0:0]
		for _, idx := range idxs {
			if want(idx) {
				kept = append(kept, idx)
			}
		}
		idxs = kept
	}
	if len(idxs) == 0 {
		return nil
	}
	d, ok := c.h.api.Catalog().Table(t)
	if !ok {
		return nil
	}
	full := row
	have := make([]bool, len(d.Columns))
	if cols == nil {
		for i := range have {
			have[i] = i < len(row)
		}
	} else {
		full = make(core.Row, len(d.Columns))
		for i := range full {
			full[i] = core.Value{Null: true}
		}
		for i, col := range cols {
			if int(col) < len(full) && i < len(row) {
				full[col] = row[i]
				have[col] = true
			}
		}
	}
	var ims []core.KeyImage
	probe := innodb.EntryWait(mode)
	for _, idx := range idxs {
		ix := d.Index(idx)
		r := core.Resource{Table: t, Index: idx, Range: &core.ImageRange{}}
		if ix != nil && covered(have, ix.Columns) {
			if ims == nil {
				all, err := c.onImages(t, []core.Row{full})
				if err != nil {
					return err
				}
				if len(all) > 0 {
					ims = all[0]
				}
			}
			if img := imageAt(ims, idx); img != nil {
				r = innodb.Point(t, idx, img)
			}
		}
		if err := c.lock(s, r, probe); err != nil {
			return err
		}
	}
	return nil
}

func covered(have []bool, cols []uint16) bool {
	for _, col := range cols {
		if int(col) >= len(have) || !have[col] {
			return false
		}
	}
	return true
}

// unlockRow is handler::unlock_row (plan 03 L3): mysqld found that the row a
// locking read just returned does not match the WHERE. At READ COMMITTED and
// READ UNCOMMITTED InnoDB then releases the record lock that read newly
// created on the row, in the read's mode, and nothing else
// (row_prebuilt_t::try_unlock, new_rec_lock): a lock the transaction already
// held, or a write lock the same statement took on the row, stays. newLock
// is the mode the read created (RowRef.new_lock, see noteLock); vdb.so sends
// Unlock only for such rows. At REPEATABLE READ and SERIALIZABLE InnoDB keeps
// the lock, and so does VDB, whatever the flag says.
func (c *conn) unlockRow(s core.Session, t core.TableID, ref core.RowRef, newLock protocol.LockMode) {
	if !c.iso.ReleasesUnmatched() {
		return
	}
	mode, ok := innodb.Record(newLock)
	if !ok {
		return
	}
	c.h.api.Locks().ReleaseMode(s, innodb.Row(t, ref.SourceKey, ref.PK.Image), mode, c.stmtMark)
}

// noteLock records, for the current request's replies, whether the read in
// protocol mode m newly created the record lock on the row with lock key
// key. A later read of the same row in the request overwrites it.
func (c *conn) noteLock(key core.KeyImage, m protocol.LockMode, created bool) {
	if !c.iso.ReleasesUnmatched() {
		// REPEATABLE READ and SERIALIZABLE keep every lock: no Unlock to ask for.
		return
	}
	if c.newLocks == nil {
		c.newLocks = map[string]protocol.LockMode{}
	}
	if created {
		c.newLocks[string(key)] = m
	} else {
		delete(c.newLocks, string(key))
	}
}

// marks returns, for each ref of b, the mode of the record lock the read
// newly created on that row (RowRef.new_lock), or nil when there is none.
func (c *conn) marks(b core.Batch) []protocol.LockMode {
	if len(c.newLocks) == 0 {
		return nil
	}
	var out []protocol.LockMode
	for i, r := range b.Refs {
		if m, ok := c.newLocks[string(core.LockKey(r.SourceKey, r.PK.Image))]; ok {
			if out == nil {
				out = make([]protocol.LockMode, len(b.Refs))
			}
			out[i] = m
		}
	}
	return out
}

// wire is newWireBatch with the refs' new_lock marks.
func (c *conn) wire(field protowire.Number, b core.Batch, end bool) *wireBatch {
	return newWireBatchMarked(field, b, end, c.marks(b))
}

// deleteAll is a DELETE without WHERE (Source scaling plan 07, SD-1). It
// declines, so mysqld deletes row by row, when the table has foreign-key
// children to check or cascade. Otherwise it locks the whole table as InnoDB
// does by locking every row: a lock on every row at READ COMMITTED and READ
// UNCOMMITTED, where inserts of new rows go ahead, and next-key locks over
// the whole index at REPEATABLE READ and SERIALIZABLE, where they wait. Then
// vdb-core detaches the table inside the transaction.
func (c *conn) deleteAll(s core.Session, t core.TableID) (*protocol.DeleteAllReply, error) {
	d, ok := c.h.api.Catalog().Table(t)
	if !ok {
		return nil, &core.Error{Code: core.ErrRefused, Table: t, Msg: "unknown table"}
	}
	if c.fkChecks && c.fk(s).HasChildren(&d) {
		return &protocol.DeleteAllReply{Declined: true}, nil
	}
	mode := innodb.RecordX
	if c.iso.Gaps() {
		mode = innodb.IntervalX
	}
	if err := c.lock(s, core.Resource{Table: t, Whole: true}, mode); err != nil {
		return nil, err
	}
	n, declined, err := s.DeleteAll(t)
	if err != nil {
		return nil, err
	}
	return &protocol.DeleteAllReply{Deleted: n, Declined: declined}, nil
}

// intervalLocks locks every scanned range of a locking scan at REPEATABLE
// READ and SERIALIZABLE: next-key locks over the merged index, stored as one
// interval per range (D-69). An index whose order is not proven gets a
// whole-index lock (D-64).
func (c *conn) intervalLocks(s core.Session, req core.ScanRequest, m protocol.LockMode) error {
	mode, ok := innodb.Record(m)
	if !ok || !c.iso.Gaps() {
		return nil
	}
	d, found := c.h.api.Catalog().Table(req.Table)
	if !found {
		return nil
	}
	ix := d.Index(req.Index)
	ranges := req.Ranges
	if len(ranges) == 0 || (ix != nil && !ix.OrderProven) {
		ranges = []core.KeyRange{{}}
	}
	for _, r := range ranges {
		if err := c.lock(s, innodb.Span(d.ID, req.Index, r.Images), innodb.Interval(mode)); err != nil {
			return err
		}
	}
	return nil
}

func nullIn(row core.Row, cols []uint16) bool {
	for _, col := range cols {
		if int(col) >= len(row) || row[col].Null {
			return true
		}
	}
	return false
}

func imageAt(ims []core.KeyImage, id core.IndexID) core.KeyImage {
	if int(id) < len(ims) {
		return ims[id]
	}
	return nil
}

// insertLocks takes an insert intention and a record lock on every unique
// key of a new row, and an insert intention on every other index (D-64).
func (c *conn) insertLocks(s core.Session, d *core.TableDef, row core.Row, ims []core.KeyImage) error {
	whole := d.Identity == core.WholeRow
	for _, ix := range d.Indexes {
		if !(ix.Unique || ix.Primary) {
			continue
		}
		img := imageAt(ims, ix.ID)
		if img == nil || nullIn(row, ix.Columns) {
			continue
		}
		if err := c.lock(s, innodb.Point(d.ID, ix.ID, img), innodb.InsertIntention); err != nil {
			return err
		}
		res := innodb.UniqueKey(d.ID, ix.ID, img)
		if ix.ID == d.Identity && !whole {
			res = innodb.Row(d.ID, nil, img)
		}
		if err := c.lock(s, res, innodb.RecordX); err != nil {
			return err
		}
	}
	for _, ix := range d.Indexes {
		if ix.Unique || ix.Primary {
			continue
		}
		if img := imageAt(ims, ix.ID); img != nil {
			if err := c.lock(s, innodb.Point(d.ID, ix.ID, img), innodb.InsertIntention); err != nil {
				return err
			}
		}
	}
	return nil
}

// fk returns the foreign-key checker for this connection.
func (c *conn) fk(s core.Session) *fk.Checker {
	return &fk.Checker{API: c.h.api, W: c, Images: c.onImages, Wait: c.onWait, Probe: c.probeEntries}
}

// insertRows locks and checks each row, then inserts the rows that passed
// in one core call. It returns one error per row (nil when the row went in),
// in the rows' order, so each failure is reported at the row where it occurs.
func (c *conn) insertRows(s core.Session, t core.TableID, rows []core.Row, ims [][]core.KeyImage) ([]error, error) {
	out := make([]error, len(rows))
	d, ok := c.h.api.Catalog().Table(t)
	var keep []int
	for i, row := range rows {
		var im []core.KeyImage
		if i < len(ims) {
			im = ims[i]
		}
		if ok {
			if err := c.insertLocks(s, &d, row, im); err != nil {
				return out, err
			}
			if c.fkChecks {
				if err := c.checkParents(s, &d, row, im); err != nil {
					if !perRow(err) {
						return out, err
					}
					out[i] = err
					continue
				}
			}
		}
		keep = append(keep, i)
	}
	if len(keep) == 0 {
		return out, nil
	}
	subRows := make([]core.Row, len(keep))
	subIms := make([][]core.KeyImage, len(keep))
	for j, i := range keep {
		subRows[j] = rows[i]
		if i < len(ims) {
			subIms[j] = ims[i]
		}
	}
	res, err := s.Insert(t, subRows, subIms)
	if err != nil {
		return out, err
	}
	for j, i := range keep {
		if j < len(res) && res[j].Err != nil {
			out[i] = res[j].Err
		}
	}
	return out, nil
}

func (c *conn) checkParents(s core.Session, d *core.TableDef, row core.Row, ims []core.KeyImage) error {
	chk := c.fk(s)
	for _, f := range mysqldef.Tab(d).ForeignKeys {
		if err := chk.CheckParent(s, d, f, row, ims); err != nil {
			return err
		}
	}
	return nil
}

// perRow reports whether an error belongs to one row (a foreign-key failure
// or a refusal) rather than ending the whole call (a lock wait, a kill).
func perRow(err error) bool {
	if _, ok := err.(*fk.Error); ok {
		return true
	}
	ce, ok := err.(*core.Error)
	return ok && ce.Code == core.ErrRefused
}

// UpdateRow updates a row with InnoDB's locks and foreign-key rules; depth is
// the cascade depth (0 for a client statement).
func (c *conn) UpdateRow(s core.Session, t core.TableID, old core.RowRef, oldRow, row core.Row, images []core.KeyImage, depth int, via string) error {
	if err := c.lock(s, innodb.Row(t, old.SourceKey, old.PK.Image), innodb.RecordX); err != nil {
		return err
	}
	d, ok := c.h.api.Catalog().Table(t)
	if ok && oldRow != nil {
		if err := c.probeEntries(s, t, oldRow, nil, innodb.RecordX, changed(&d, oldRow, row)); err != nil {
			return err
		}
	}
	if ok && d.Identity != core.WholeRow {
		if img := imageAt(images, d.Identity); img != nil && !bytes.Equal(img, old.PK.Image) {
			if err := c.lock(s, innodb.Row(t, nil, img), innodb.RecordX); err != nil {
				return err
			}
		}
	}
	if ok && (c.fkChecks || depth > 0) {
		chk := c.fk(s)
		for _, f := range mysqldef.Tab(&d).ForeignKeys {
			if f.Name == via || (oldRow != nil && sameCols(oldRow, row, f.Columns)) {
				continue
			}
			if err := chk.CheckParent(s, &d, f, row, images); err != nil {
				return err
			}
		}
		if err := chk.OnParentChange(s, &d, oldRow, row, depth); err != nil {
			return err
		}
	}
	return s.Update(t, old, row, images)
}

// DeleteRow deletes a row with InnoDB's locks and foreign-key rules.
func (c *conn) DeleteRow(s core.Session, t core.TableID, old core.RowRef, oldRow core.Row, depth int) error {
	if err := c.lock(s, innodb.Row(t, old.SourceKey, old.PK.Image), innodb.RecordX); err != nil {
		return err
	}
	if oldRow != nil {
		if err := c.probeEntries(s, t, oldRow, nil, innodb.RecordX, nil); err != nil {
			return err
		}
	}
	if d, ok := c.h.api.Catalog().Table(t); ok && (c.fkChecks || depth > 0) {
		if err := c.fk(s).OnParentChange(s, &d, oldRow, nil, depth); err != nil {
			return err
		}
	}
	return s.Delete(t, old)
}

func sameCols(a, b core.Row, cols []uint16) bool {
	for _, col := range cols {
		if int(col) >= len(a) || int(col) >= len(b) {
			return false
		}
		if a[col].Null != b[col].Null || !bytes.Equal(a[col].B, b[col].B) {
			return false
		}
	}
	return true
}
