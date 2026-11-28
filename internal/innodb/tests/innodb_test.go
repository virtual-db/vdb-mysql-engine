// Package innodb_test — tests/innodb_test.go
//
// InnoDB's compatibility matrix as a table test, and the isolation mapping
// (plan doc 09 §3.3, §6).
package innodb_test

import (
	"testing"

	core "github.com/virtual-db/vdb-core"

	. "github.com/virtual-db/vdb-mysql-engine/internal/innodb"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

func TestMatrix(t *testing.T) {
	m := Modes()
	cases := []struct {
		held, wanted core.LockMode
		conflict     bool
	}{
		{RecordS, RecordS, false},
		{RecordS, RecordX, true},
		{RecordX, RecordS, true},
		{Gap, Gap, false},
		{Gap, RecordX, false},
		{RecordX, Gap, false},
		{Gap, InsertIntention, true},
		{IntervalS, InsertIntention, true},
		{IntervalX, InsertIntention, true},
		{RecordX, InsertIntention, false},
		{IntervalS, RecordS, false},
		{IntervalS, RecordX, true},
		{IntervalX, IntervalS, true},
		// A row inside the reader's own interval waits like a record lock.
		{RecordS, RecordWaitS, false},
		{RecordX, RecordWaitS, true},
		{RecordS, RecordWaitX, true},
		// Entry probes wait for other transactions' intervals only.
		{IntervalS, EntryWaitS, false},
		{IntervalX, EntryWaitS, true},
		{IntervalS, EntryWaitX, true},
		{IntervalX, EntryWaitX, true},
		{Gap, EntryWaitX, false},
		{RecordX, EntryWaitX, false},
	}
	for i, c := range cases {
		if got := m.Conflicts(c.held, c.wanted, core.Resource{}, core.Resource{}); got != c.conflict {
			t.Errorf("case %d (%s held, %s wanted): conflict = %v, want %v", i, m.Names[c.held], m.Names[c.wanted], got, c.conflict)
		}
	}
	for _, mode := range []core.LockMode{InsertIntention, RecordWaitS, RecordWaitX, EntryWaitS, EntryWaitX} {
		if !m.WaitOnly(mode) {
			t.Errorf("%s must be wait-only", m.Names[mode])
		}
	}
	for _, mode := range []core.LockMode{RecordS, RecordX, Gap, IntervalS, IntervalX} {
		if m.WaitOnly(mode) {
			t.Errorf("%s must be kept", m.Names[mode])
		}
	}
	if Wait(RecordX) != RecordWaitX || Wait(RecordS) != RecordWaitS || EntryWait(RecordX) != EntryWaitX || EntryWait(RecordS) != EntryWaitS {
		t.Error("Wait and EntryWait map record modes to their wait-only modes")
	}
	if !m.Covers(RecordX, RecordS) || m.Covers(RecordS, RecordX) {
		t.Error("an exclusive record lock covers a shared one, not the reverse")
	}
}

func TestIsolation(t *testing.T) {
	cases := []struct {
		level protocol.Isolation
		scope core.SnapshotScope
		view  core.ReadView
		gaps  bool
		// unmatched: handler::unlock_row releases a row the WHERE rejected
		// (InnoDB: READ COMMITTED and READ UNCOMMITTED only).
		unmatched bool
	}{
		{protocol.Isolation_READ_UNCOMMITTED, core.PerStatement, core.ViewUncommitted, false, true},
		{protocol.Isolation_READ_COMMITTED, core.PerStatement, core.ViewSnapshot, false, true},
		{protocol.Isolation_REPEATABLE_READ, core.PerTransaction, core.ViewSnapshot, true, false},
		{protocol.Isolation_SERIALIZABLE, core.PerTransaction, core.ViewSnapshot, true, false},
	}
	for _, c := range cases {
		i := Isolation{Level: c.level}
		if i.Scope() != c.scope || i.View() != c.view || i.Gaps() != c.gaps || i.ReleaseOnStatementRollback() == c.gaps ||
			i.ReleasesUnmatched() != c.unmatched {
			t.Errorf("%v: scope %v view %v gaps %v releases unmatched %v", c.level, i.Scope(), i.View(), i.Gaps(), i.ReleasesUnmatched())
		}
	}
}

func TestReadLock(t *testing.T) {
	none, shared, excl := protocol.LockMode_LOCK_NONE, protocol.LockMode_LOCK_SHARED, protocol.LockMode_LOCK_EXCLUSIVE
	levels := []protocol.Isolation{protocol.Isolation_READ_UNCOMMITTED, protocol.Isolation_READ_COMMITTED,
		protocol.Isolation_REPEATABLE_READ, protocol.Isolation_SERIALIZABLE}
	for _, level := range levels {
		for _, auto := range []bool{true, false} {
			i := Isolation{Level: level, Autocommit: auto}
			want := none
			if level == protocol.Isolation_SERIALIZABLE && !auto {
				want = shared
			}
			if got := i.ReadLock(none); got != want {
				t.Errorf("%v autocommit=%v: plain read takes %v, want %v", level, auto, got, want)
			}
			if i.PlainReadsLock() != (want == shared) {
				t.Errorf("%v autocommit=%v: PlainReadsLock %v", level, auto, i.PlainReadsLock())
			}
			if i.ReadLock(shared) != shared || i.ReadLock(excl) != excl {
				t.Errorf("%v autocommit=%v: locking reads keep their mode", level, auto)
			}
		}
	}
}
