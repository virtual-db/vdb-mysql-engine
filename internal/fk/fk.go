// Package fk enforces foreign keys as InnoDB does (implementation plan doc 09
// §3.5, product doc 10 §6, D-61), over merged rows read through vdb-core:
// parent checks with a shared lock on the parent; RESTRICT, NO ACTION,
// CASCADE and SET NULL inside the write that triggered them; a cascade depth
// limit of 15; and validation of existing rows for ADD FOREIGN KEY. Rows a
// cascade writes get their sort images from vdb.so (D-86).
package fk

import (
	"bytes"
	"io"
	"strings"

	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"

	core "github.com/virtual-db/vdb-core"

	"github.com/virtual-db/vdb-mysql-engine/internal/innodb"
)

// MaxDepth is InnoDB's cascade depth limit.
const MaxDepth = 15

// Code is a foreign-key error.
type Code uint8

const (
	NoReferencedRow Code = iota + 1
	RowIsReferenced
	Depth
)

// Error is a foreign-key failure, mapped to HA_ERR_* by the data handler.
type Error struct {
	Code       Code
	Constraint string
	Msg        string
}

func (e *Error) Error() string { return e.Msg }

// Writer is the data handler's own write path, so a cascaded change takes
// its locks and runs these rules again, one level deeper.
type Writer interface {
	// UpdateRow's via names the foreign key whose cascade made the change; its
	// parent check is skipped, as InnoDB skips it (the parent is changing).
	UpdateRow(s core.Session, t core.TableID, old core.RowRef, oldRow, row core.Row, images []core.KeyImage, depth int, via string) error
	DeleteRow(s core.Session, t core.TableID, old core.RowRef, oldRow core.Row, depth int) error
}

// ImageFunc computes the sort image of every index of rows (vdb.so, D-86).
type ImageFunc func(t core.TableID, rows []core.Row) ([][]core.KeyImage, error)

// Checker runs foreign-key rules for one session's statement.
type Checker struct {
	API    core.DriverAPI
	W      Writer
	Images ImageFunc
	Wait   core.WaitFunc
	// Probe makes a record lock wait for other transactions' interval locks
	// on the row's entries in the indexes want selects (vdb-mysql-engine#6);
	// nil skips it.
	Probe func(s core.Session, t core.TableID, row core.Row, cols core.ColumnSet, mode core.LockMode, want func(core.IndexID) bool) error
}

func values(row core.Row, cols []uint16) (core.Row, bool) {
	out := make(core.Row, len(cols))
	for i, c := range cols {
		if int(c) >= len(row) || row[c].Null {
			return nil, false
		}
		out[i] = row[c]
	}
	return out, true
}

func prefixLen(ix *core.IndexDef, parts int) int {
	if ix == nil || len(mysqldef.Idx(ix).PartLens) < parts {
		return -1
	}
	n := 0
	for i := 0; i < parts; i++ {
		n += int(mysqldef.Idx(ix).PartLens[i])
	}
	return n
}

func cut(img core.KeyImage, n int) core.KeyImage {
	if n < 0 || n > len(img) {
		return img
	}
	return img[:n]
}

func refused(t core.TableID, name string) error {
	return &core.Error{Code: core.ErrRefused, Table: t,
		Msg: "foreign key " + name + " uses a collation not proven for equality; set foreign_key_checks=0 to write without the check (D-62)"}
}

// rowLocks locks each row a read through index idx returns in mode, through
// the lock table; the probe, as on InnoDB, covers only idx.
func (c *Checker) rowLocks(s core.Session, idx core.IndexID, mode core.LockMode) *core.ReadLocks {
	rl := &core.ReadLocks{Row: func(t core.TableID, key core.KeyImage) error {
		return c.API.Locks().Acquire(s, core.Resource{Table: t, Point: key}, mode, c.Wait)
	}}
	if c.Probe != nil {
		rl.Probe = func(t core.TableID, row core.Row, cols core.ColumnSet) error {
			return c.Probe(s, t, row, cols, mode, func(i core.IndexID) bool { return i == idx })
		}
	}
	return rl
}

// CheckParent checks that a child row's parent exists, taking a shared lock
// on it. A NULL in any foreign-key column skips the check.
func (c *Checker) CheckParent(s core.Session, child *core.TableDef, fk mysqldef.ForeignKey, row core.Row, images []core.KeyImage) error {
	vals, ok := values(row, fk.Columns)
	if !ok {
		return nil
	}
	parent, ok := c.API.Catalog().TableByName(mysqldef.NS(fk.ParentDB), fk.ParentTable)
	if !ok {
		return &Error{Code: NoReferencedRow, Constraint: fk.Name, Msg: "Cannot add or update a child row: a foreign key constraint fails (parent table is missing)"}
	}
	pid, found := mysqldef.ParentIndex(&parent, fk)
	pix := parent.Index(pid)
	if !found || pix == nil {
		return &Error{Code: NoReferencedRow, Constraint: fk.Name, Msg: "Cannot add or update a child row: a foreign key constraint fails (parent key is missing)"}
	}
	if !pix.EqualityProven {
		return refused(child.ID, fk.Name)
	}
	img, err := c.keyImage(&parent, pix.ID, pix.Columns, vals)
	if err != nil {
		return err
	}
	cur, err := s.ByColumns(parent.ID, pix.ID, img, pix.Columns, vals, pix.Columns, c.rowLocks(s, pix.ID, innodb.RecordS))
	if err != nil {
		return err
	}
	// One parent row is enough.
	exists, err := hasRow(cur)
	if err != nil {
		return err
	}
	if !exists {
		return &Error{Code: NoReferencedRow, Constraint: fk.Name, Msg: "Cannot add or update a child row: a foreign key constraint fails"}
	}
	return nil
}

// childFK is a foreign key with its child table.
type childFK struct {
	child core.TableDef
	fk    mysqldef.ForeignKey
}

// HasChildren reports whether a foreign key references the table.
func (c *Checker) HasChildren(parent *core.TableDef) bool { return len(c.children(parent)) > 0 }

func (c *Checker) children(parent *core.TableDef) []childFK {
	var out []childFK
	for _, t := range c.API.Catalog().Tables() {
		for _, f := range mysqldef.Tab(&t).ForeignKeys {
			if strings.EqualFold(f.ParentDB, mysqldef.DB(parent)) && strings.EqualFold(f.ParentTable, parent.Name) {
				out = append(out, childFK{child: t, fk: f})
			}
		}
	}
	return out
}

// OnParentChange applies every foreign key that references a changed parent
// row; newRow is nil for a delete. Children are found through the child's
// supporting index in the Delta and by value on the Source.
func (c *Checker) OnParentChange(s core.Session, parent *core.TableDef, oldRow, newRow core.Row, depth int) error {
	kids := c.children(parent)
	if len(kids) == 0 || oldRow == nil {
		return nil
	}
	for _, cf := range kids {
		f := cf.fk
		pid, found := mysqldef.ParentIndex(parent, f)
		pix := parent.Index(pid)
		if !found || pix == nil {
			continue
		}
		oldVals, ok := values(oldRow, pix.Columns)
		if !ok {
			continue
		}
		if newRow != nil {
			if newVals, ok := values(newRow, pix.Columns); ok && equal(oldVals, newVals) {
				continue // the referenced key did not change
			}
		}
		if !pix.EqualityProven {
			return refused(parent.ID, f.Name)
		}
		action := f.OnDelete
		if newRow != nil {
			action = f.OnUpdate
		}
		cdef := cf.child
		img, err := c.keyImage(&cdef, f.ChildIndex, f.Columns, oldVals)
		if err != nil {
			return err
		}
		restrict := action == mysqldef.FKRestrict || action == mysqldef.FKNoAction
		var locks *core.ReadLocks
		if !restrict {
			locks = c.rowLocks(s, f.ChildIndex, innodb.RecordX)
		}
		cur, err := s.ByColumns(cdef.ID, f.ChildIndex, img, f.Columns, oldVals, all(&cdef), locks)
		if err != nil {
			return err
		}
		if restrict {
			// RESTRICT stops at the first child row.
			found, err := hasRow(cur)
			if err != nil {
				return err
			}
			if found {
				return &Error{Code: RowIsReferenced, Constraint: f.Name, Msg: "Cannot delete or update a parent row: a foreign key constraint fails"}
			}
			continue
		}
		if err := c.cascade(s, cur, &cdef, f, pix, newRow, action, depth); err != nil {
			return err
		}
	}
	return nil
}

// hasRow reports whether the cursor has a row, reading at most one, and closes it.
func hasRow(cur core.Cursor) (bool, error) {
	defer cur.Close()
	b, err := cur.Next(1, 0)
	if err != nil && err != io.EOF {
		return false, err
	}
	return len(b.Rows) > 0, nil
}

// cascade applies a CASCADE or SET NULL action to the children the cursor
// yields, one batch at a time (Source scaling plan K3), and closes it.
func (c *Checker) cascade(s core.Session, cur core.Cursor, cdef *core.TableDef, f mysqldef.ForeignKey, pix *core.IndexDef,
	newRow core.Row, action mysqldef.FKAction, depth int) error {
	defer cur.Close()
	for {
		b, err := cur.Next(0, 0)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(b.Rows) > 0 && depth+1 > MaxDepth {
			return &Error{Code: Depth, Constraint: f.Name, Msg: "Foreign key cascade delete/update exceeds max depth of 15."}
		}
		if err := c.applyChildren(s, b, cdef, f, pix, newRow, action, depth); err != nil {
			return err
		}
	}
}

// applyChildren applies the action to one batch of children.
func (c *Checker) applyChildren(s core.Session, b core.Batch, cdef *core.TableDef, f mysqldef.ForeignKey, pix *core.IndexDef,
	newRow core.Row, action mysqldef.FKAction, depth int) error {
	for i, row := range b.Rows {
		ref := b.Refs[i]
		if action == mysqldef.FKCascade && newRow == nil {
			if err := c.W.DeleteRow(s, cdef.ID, ref, row, depth+1); err != nil {
				return err
			}
			continue
		}
		nr := core.Row(append([]core.Value(nil), row...))
		newVals, _ := values(newRow, pix.Columns)
		for j, col := range f.Columns {
			if action == mysqldef.FKSetNull || newVals == nil {
				nr[col] = core.Value{Null: true}
			} else {
				nr[col] = newVals[j]
			}
		}
		if c.Images == nil {
			return &core.Error{Code: core.ErrInternal, Table: cdef.ID, Msg: "no image callback for a cascaded update"}
		}
		ims, err := c.Images(cdef.ID, []core.Row{nr})
		if err != nil {
			return err
		}
		if err := c.W.UpdateRow(s, cdef.ID, ref, row, nr, ims[0], depth+1, f.Name); err != nil {
			return err
		}
	}
	return nil
}

// keyImage is the sort image of the leading key parts of index idx of table
// t for the values vals of its columns cols, as vdb.so computes it for that
// table: the referencing and the referenced key can differ in form (a
// nullable child column carries a NULL indicator the parent key does not),
// so each side's image comes from its own index. Nil means no Delta lookup.
func (c *Checker) keyImage(t *core.TableDef, idx core.IndexID, cols []uint16, vals core.Row) (core.KeyImage, error) {
	ix := t.Index(idx)
	if c.Images == nil || ix == nil {
		return nil, nil
	}
	row := make(core.Row, len(t.Columns))
	for i := range row {
		row[i] = core.Value{Null: true}
	}
	for i, col := range cols {
		if int(col) < len(row) && i < len(vals) {
			row[col] = vals[i]
		}
	}
	ims, err := c.Images(t.ID, []core.Row{row})
	if err != nil {
		return nil, err
	}
	if len(ims) == 0 || int(idx) >= len(ims[0]) {
		return nil, nil
	}
	return cut(ims[0][idx], prefixLen(ix, len(cols))), nil
}

func all(d *core.TableDef) core.ColumnSet {
	out := make(core.ColumnSet, len(d.Columns))
	for i := range out {
		out[i] = uint16(i)
	}
	return out
}

func equal(a, b core.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Null != b[i].Null || !bytes.Equal(a[i].B, b[i].B) {
			return false
		}
	}
	return true
}

// ValidateExisting checks every existing child row for ADD FOREIGN KEY with
// checks on: it streams the merged child table and fails on the first
// orphan. Nothing is stored (product doc 10 §6.4).
func (c *Checker) ValidateExisting(s core.Session, cdef *core.TableDef, f mysqldef.ForeignKey) error {
	h, err := s.Scan(core.ScanRequest{Table: cdef.ID, Index: cdef.Identity, Columns: all(cdef)})
	if err != nil {
		return err
	}
	defer h.Close()
	for {
		b, err := h.Merged().Next(1000, 0)
		if len(b.Rows) > 0 {
			var images [][]core.KeyImage
			if c.Images != nil {
				if images, err = c.Images(cdef.ID, b.Rows); err != nil {
					return err
				}
			}
			for i, row := range b.Rows {
				var ims []core.KeyImage
				if i < len(images) {
					ims = images[i]
				}
				if err := c.CheckParent(s, cdef, f, row, ims); err != nil {
					return err
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
