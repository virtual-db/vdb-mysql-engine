// Package innodb holds InnoDB's lock and isolation rules on vdb-core's
// mechanisms (implementation plan doc 09 §3.3, D-89; product doc 07 §5,
// D-63, D-64, D-69). vdb-core provides a lock table, snapshot scopes and read
// views; this package defines the lock modes and their compatibility matrix,
// says which locks each read and write takes, and maps isolation levels.
//
// Resources follow one convention, so both sides of a conflict name the same
// resource: row locks live in index 0 of the table, keyed by core.LockKey;
// unique-key locks of other indexes too, keyed by UniqueKey; gap, interval,
// insert-intention and whole-index locks live in the index they cover, keyed
// by sort image.
package innodb

import (
	core "github.com/virtual-db/vdb-core"

	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

// Lock modes.
const (
	RecordS         core.LockMode = iota // shared record lock
	RecordX                              // exclusive record lock
	Gap                                  // gap lock: blocks inserts only
	IntervalS                            // the record and gap locks of every row in a range, shared
	IntervalX                            // the same, exclusive
	InsertIntention                      // waits for gap and interval locks; not kept
	// RecordWaitS and RecordWaitX wait like a record lock and are not kept:
	// a row a locking scan reads inside its own interval (plan 03 L1).
	RecordWaitS
	RecordWaitX
	// EntryWaitS and EntryWaitX wait at a row's image on one index for other
	// transactions' interval locks covering it, and are not kept: every
	// record lock probes them, so a held range blocks the writers of its
	// rows (vdb-mysql-engine#6).
	EntryWaitS
	EntryWaitX
)

// Modes is the definition passed to core.Locks.Define once at start.
func Modes() core.LockModes {
	return core.LockModes{
		Names: []string{"RECORD_S", "RECORD_X", "GAP", "INTERVAL_S", "INTERVAL_X", "INSERT_INTENTION",
			"RECORD_WAIT_S", "RECORD_WAIT_X", "ENTRY_WAIT_S", "ENTRY_WAIT_X"},
		Conflicts: conflicts,
		Covers:    covers,
		WaitOnly:  waitOnly,
	}
}

func waitOnly(m core.LockMode) bool { return m >= InsertIntention }

func exclusive(m core.LockMode) bool {
	return m == RecordX || m == IntervalX || m == RecordWaitX || m == EntryWaitX
}

// Wait returns the wait-only mode of a record lock mode, for a row inside an
// interval the transaction holds.
func Wait(record core.LockMode) core.LockMode {
	if record == RecordX {
		return RecordWaitX
	}
	return RecordWaitS
}

// EntryWait returns the mode that probes a row's index entries for a record
// lock mode.
func EntryWait(record core.LockMode) core.LockMode {
	if record == RecordX {
		return EntryWaitX
	}
	return EntryWaitS
}

// conflicts is InnoDB's compatibility matrix for overlapping resources held
// by another transaction: gap locks never conflict with each other; an
// insert intention waits for any gap or interval lock covering its image;
// record and interval locks follow shared and exclusive rules.
func conflicts(held, wanted core.LockMode, _, _ core.Resource) bool {
	switch {
	case wanted == InsertIntention:
		return held == Gap || held == IntervalS || held == IntervalX
	case wanted == Gap || held == Gap || held == InsertIntention:
		return false
	case wanted == EntryWaitS || wanted == EntryWaitX:
		// Only intervals sit in an index's slot besides insert intentions.
		return (held == IntervalS || held == IntervalX) && (exclusive(held) || exclusive(wanted))
	default:
		return exclusive(held) || exclusive(wanted)
	}
}

func covers(held, wanted core.LockMode) bool {
	switch wanted {
	case RecordS:
		return held == RecordS || held == RecordX
	case IntervalS:
		return held == IntervalS || held == IntervalX
	}
	return held == wanted
}

// Record returns the record lock mode for a protocol lock mode, and false for
// a plain read.
func Record(m protocol.LockMode) (core.LockMode, bool) {
	switch m {
	case protocol.LockMode_LOCK_SHARED:
		return RecordS, true
	case protocol.LockMode_LOCK_EXCLUSIVE:
		return RecordX, true
	}
	return 0, false
}

// Interval returns the interval lock mode matching a record lock mode.
func Interval(record core.LockMode) core.LockMode {
	if record == RecordX {
		return IntervalX
	}
	return IntervalS
}

// Row is the resource of a row's record lock.
func Row(t core.TableID, sk core.SourceKey, img core.KeyImage) core.Resource {
	return core.Resource{Table: t, Point: core.LockKey(sk, img)}
}

// UniqueKey is the resource of a unique key's record lock on an index that is
// not the identity key.
func UniqueKey(t core.TableID, idx core.IndexID, img core.KeyImage) core.Resource {
	k := core.KeyImage{'u', byte(idx >> 8), byte(idx)}
	return core.Resource{Table: t, Point: append(k, img...)}
}

// Point is the resource of an insert intention at one image of an index.
func Point(t core.TableID, idx core.IndexID, img core.KeyImage) core.Resource {
	return core.Resource{Table: t, Index: idx, Point: img}
}

// Span is the resource of a gap or interval lock over a range of an index.
func Span(t core.TableID, idx core.IndexID, r core.ImageRange) core.Resource {
	return core.Resource{Table: t, Index: idx, Range: &r}
}

// Isolation is a session's isolation level and what it means on the core.
// Autocommit says the statement runs outside a multi-statement transaction:
// autocommit on and no BEGIN (mysqld's OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN
// both clear).
type Isolation struct {
	Level      protocol.Isolation
	Autocommit bool
}

// ReadLock returns the lock mode a read through mysqld's lock mode m takes.
// At SERIALIZABLE inside a multi-statement transaction InnoDB turns every
// plain read into a shared locking read, as if it said LOCK IN SHARE MODE
// (ha_innobase::external_lock); a plain read in autocommit stays a
// consistent read. Every other read keeps m.
func (i Isolation) ReadLock(m protocol.LockMode) protocol.LockMode {
	if m == protocol.LockMode_LOCK_NONE && i.Level == protocol.Isolation_SERIALIZABLE && !i.Autocommit {
		return protocol.LockMode_LOCK_SHARED
	}
	return m
}

// PlainReadsLock reports whether plain reads are locking reads (ReadLock).
// An exact count from the table's view then cannot stand in for the read:
// InnoDB answers records() with a scan that takes shared next-key locks on
// every row (row_scan_index_for_mysql with LOCK_S), so the engine declines
// the count and mysqld counts through a scan.
func (i Isolation) PlainReadsLock() bool {
	return i.ReadLock(protocol.LockMode_LOCK_NONE) != protocol.LockMode_LOCK_NONE
}

// Scope is the snapshot scope: one pair per statement at READ COMMITTED and
// READ UNCOMMITTED, one per transaction otherwise (product doc 07 §4).
func (i Isolation) Scope() core.SnapshotScope {
	if i.Level == protocol.Isolation_READ_COMMITTED || i.Level == protocol.Isolation_READ_UNCOMMITTED {
		return core.PerStatement
	}
	return core.PerTransaction
}

// View is the plain-read view: READ UNCOMMITTED reads other transactions'
// uncommitted writes (D-63).
func (i Isolation) View() core.ReadView {
	if i.Level == protocol.Isolation_READ_UNCOMMITTED {
		return core.ViewUncommitted
	}
	return core.ViewSnapshot
}

// Gaps reports whether locking reads take gap, next-key and interval locks:
// at REPEATABLE READ and SERIALIZABLE only (D-64).
func (i Isolation) Gaps() bool {
	return i.Level == protocol.Isolation_REPEATABLE_READ || i.Level == protocol.Isolation_SERIALIZABLE
}

// ReleasesUnmatched reports whether handler::unlock_row releases the lock of a
// row a locking read returned and the WHERE rejected: at READ COMMITTED and
// READ UNCOMMITTED only. At REPEATABLE READ and SERIALIZABLE InnoDB keeps it
// until the transaction ends (plan 03, L3).
func (i Isolation) ReleasesUnmatched() bool { return !i.Gaps() }

// ReleaseOnStatementRollback reports whether a failed statement's locks go
// with it: at READ COMMITTED and READ UNCOMMITTED, as InnoDB does.
func (i Isolation) ReleaseOnStatementRollback() bool { return !i.Gaps() }
