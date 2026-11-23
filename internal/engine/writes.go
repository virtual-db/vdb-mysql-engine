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

	"github.com/virtual-db/vdb-mysql-engine/internal/fk"
	"github.com/virtual-db/vdb-mysql-engine/internal/innodb"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

func (c *conn) lock(s core.Session, r core.Resource, mode core.LockMode) error {
	return c.h.api.Locks().Acquire(s, r, mode, c.onWait)
}

// readLocks returns the hooks of a locking read in protocol mode m, or nil
// for a plain read. A unique lookup that finds nothing takes a gap lock at
// REPEATABLE READ and SERIALIZABLE (D-64).
func (c *conn) readLocks(s core.Session, m protocol.LockMode) *core.ReadLocks {
	mode, ok := innodb.Record(m)
	if !ok {
		return nil
	}
	rl := &core.ReadLocks{Row: func(t core.TableID, key core.KeyImage) error {
		return c.lock(s, core.Resource{Table: t, Point: key}, mode)
	}}
	if c.iso.Gaps() {
		rl.Missing = func(t core.TableID, idx core.IndexID, img core.KeyImage) error {
			return c.lock(s, innodb.Span(t, idx, core.ImageRange{Low: img, High: img, LowIncl: true, HighIncl: true}), innodb.Gap)
		}
	}
	return rl
}

// unlockRow is handler::unlock_row (plan 03 L3): mysqld found that the row a
// locking read just returned does not match the WHERE. At READ COMMITTED and
// READ UNCOMMITTED InnoDB then releases the row's record lock, unless the
// transaction held it before this statement; at REPEATABLE READ and
// SERIALIZABLE it keeps it, and so does VDB.
func (c *conn) unlockRow(s core.Session, t core.TableID, ref core.RowRef) {
	if c.iso.Gaps() {
		return
	}
	c.h.api.Locks().Release(s, innodb.Row(t, ref.SourceKey, ref.PK.Image), c.stmtMark)
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
	return &fk.Checker{API: c.h.api, W: c, Images: c.onImages, Wait: c.onWait}
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
