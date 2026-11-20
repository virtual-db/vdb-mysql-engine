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
)

// Modes is the definition passed to core.Locks.Define once at start.
func Modes() core.LockModes {
	return core.LockModes{
		Names:     []string{"RECORD_S", "RECORD_X", "GAP", "INTERVAL_S", "INTERVAL_X", "INSERT_INTENTION"},
		Conflicts: conflicts,
		Covers:    covers,
		WaitOnly:  func(m core.LockMode) bool { return m == InsertIntention },
	}
}

func exclusive(m core.LockMode) bool { return m == RecordX || m == IntervalX }

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
type Isolation struct {
	Level protocol.Isolation
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

// ReleaseOnStatementRollback reports whether a failed statement's locks go
// with it: at READ COMMITTED and READ UNCOMMITTED, as InnoDB does.
func (i Isolation) ReleaseOnStatementRollback() bool { return !i.Gaps() }
