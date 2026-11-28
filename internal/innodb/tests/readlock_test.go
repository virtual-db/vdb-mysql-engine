// Package innodb_test — tests/readlock_test.go
//
// InnoDB's select_lock_type rule (readlock.go) as a table test: the record
// lock matrix measured on stock MySQL 8.4.13 for the read side of each
// statement (handoff #18, results/innodb-locks-8.4.13.txt), plus the cases
// read from ha_innodb.cc: CHECKSUM, autocommit, LOCK TABLES, HANDLER, and
// unknown or missing facts.
package innodb_test

import (
	"testing"

	. "github.com/virtual-db/vdb-mysql-engine/internal/innodb"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

const (
	none   = protocol.LockMode_LOCK_NONE
	shared = protocol.LockMode_LOCK_SHARED
	excl   = protocol.LockMode_LOCK_EXCLUSIVE
)

var (
	ru  = protocol.Isolation_READ_UNCOMMITTED
	rc  = protocol.Isolation_READ_COMMITTED
	rr  = protocol.Isolation_REPEATABLE_READ
	ser = protocol.Isolation_SERIALIZABLE
)

// stmt is the facts of one read handle in an ordinary statement: store_lock
// with lt and cmd, then external_lock.
func stmt(lt protocol.ThrLockType, cmd protocol.SqlCommand, write bool) *protocol.ReadLockFacts {
	f := &protocol.ReadLockFacts{StoreLockType: lt, StoreSqlCommand: cmd, ExternalLock: protocol.ExternalLock_EXTERNAL_LOCK_READ}
	if write {
		f.ExternalLock = protocol.ExternalLock_EXTERNAL_LOCK_WRITE
	}
	return f
}

func TestReadLockMatrix(t *testing.T) {
	tlRead, tlNoIns := protocol.ThrLockType_TL_READ, protocol.ThrLockType_TL_READ_NO_INSERT
	// modes at RU, RC, RR, SERIALIZABLE, inside a transaction.
	cases := []struct {
		name  string
		facts *protocol.ReadLockFacts
		modes [4]protocol.LockMode
	}{
		{"SELECT", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_SELECT, false), [4]protocol.LockMode{none, none, none, shared}},
		{"SELECT INTO @x", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_SELECT, false), [4]protocol.LockMode{none, none, none, shared}},
		{"SELECT FOR SHARE", stmt(protocol.ThrLockType_TL_READ_WITH_SHARED_LOCKS, protocol.SqlCommand_SQL_COMMAND_SELECT, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"SELECT FOR UPDATE", stmt(protocol.ThrLockType_TL_WRITE, protocol.SqlCommand_SQL_COMMAND_SELECT, true), [4]protocol.LockMode{excl, excl, excl, excl}},
		{"INSERT ... SELECT (TL_READ)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_INSERT_SELECT, false), [4]protocol.LockMode{none, none, shared, shared}},
		{"INSERT ... SELECT (TL_READ_NO_INSERT)", stmt(tlNoIns, protocol.SqlCommand_SQL_COMMAND_INSERT_SELECT, false), [4]protocol.LockMode{none, none, shared, shared}},
		{"REPLACE ... SELECT", stmt(tlNoIns, protocol.SqlCommand_SQL_COMMAND_REPLACE_SELECT, false), [4]protocol.LockMode{none, none, shared, shared}},
		{"UPDATE t1 SET v=(SELECT ... t2)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_UPDATE, false), [4]protocol.LockMode{none, none, shared, shared}},
		{"CREATE TABLE ... SELECT", stmt(tlNoIns, protocol.SqlCommand_SQL_COMMAND_CREATE_TABLE, false), [4]protocol.LockMode{none, none, shared, shared}},
		{"DELETE ... IN (SELECT ... t2)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_DELETE, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"UPDATE t1 JOIN t2 (t2)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_UPDATE_MULTI, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"DELETE t1 FROM t1 JOIN t2 (t2)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_DELETE_MULTI, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"INSERT VALUES((SELECT ... t2))", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_INSERT, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"SET @x=(SELECT ...)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_OTHER, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"DO (SELECT ...)", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_OTHER, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"UPDATE write handle", stmt(protocol.ThrLockType_TL_WRITE_ALLOW_WRITE, protocol.SqlCommand_SQL_COMMAND_UPDATE, true), [4]protocol.LockMode{excl, excl, excl, excl}},
		{"DELETE write handle", stmt(protocol.ThrLockType_TL_WRITE_DEFAULT, protocol.SqlCommand_SQL_COMMAND_DELETE, true), [4]protocol.LockMode{excl, excl, excl, excl}},
		{"CHECKSUM TABLE", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_CHECKSUM, false), [4]protocol.LockMode{none, none, none, shared}},
		{"unknown command", stmt(tlRead, protocol.SqlCommand(999), false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"unspecified command", stmt(tlRead, protocol.SqlCommand_SQL_COMMAND_UNSPECIFIED, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"unknown lock type", stmt(protocol.ThrLockType(99), protocol.SqlCommand_SQL_COMMAND_SELECT, false), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"no facts (old vdb.so)", &protocol.ReadLockFacts{}, [4]protocol.LockMode{shared, shared, shared, shared}},
		{"nil facts", nil, [4]protocol.LockMode{shared, shared, shared, shared}},
		{"HANDLER ... READ", &protocol.ReadLockFacts{StoreLockType: tlRead, StoreSqlCommand: protocol.SqlCommand_SQL_COMMAND_HA_READ,
			ExternalLock: protocol.ExternalLock_EXTERNAL_LOCK_READ, ForHandler: true}, [4]protocol.LockMode{none, none, none, none}},
	}
	for _, c := range cases {
		for i, level := range []protocol.Isolation{ru, rc, rr, ser} {
			got := Isolation{Level: level}.ReadLock(c.facts)
			if got != c.modes[i] {
				t.Errorf("%s at %v: %v, want %v", c.name, level, got, c.modes[i])
			}
		}
	}
}

// TestReadLockAutocommit: SERIALIZABLE turns a plain read into a shared one
// only inside a multi-statement transaction (external_lock); a locking read
// keeps its lock in autocommit.
func TestReadLockAutocommit(t *testing.T) {
	sel := stmt(protocol.ThrLockType_TL_READ, protocol.SqlCommand_SQL_COMMAND_SELECT, false)
	ins := stmt(protocol.ThrLockType_TL_READ, protocol.SqlCommand_SQL_COMMAND_INSERT_SELECT, false)
	for _, level := range []protocol.Isolation{ru, rc, rr, ser} {
		auto := Isolation{Level: level, Autocommit: true}
		if got := auto.ReadLock(sel); got != none {
			t.Errorf("autocommit SELECT at %v: %v, want none", level, got)
		}
		want := shared
		if level == ru || level == rc {
			want = none
		}
		if got := auto.ReadLock(ins); got != want {
			t.Errorf("autocommit INSERT ... SELECT at %v: %v, want %v", level, got, want)
		}
	}
}

// TestReadLockLockTables: under LOCK TABLES each statement comes through
// start_stmt, which reads the command again; LOCK TABLES ... READ stored a
// shared lock at store_lock.
func TestReadLockLockTables(t *testing.T) {
	lockRead := func(cmd protocol.SqlCommand, lt protocol.ThrLockType) *protocol.ReadLockFacts {
		return &protocol.ReadLockFacts{StoreLockType: protocol.ThrLockType_TL_READ, StoreSqlCommand: protocol.SqlCommand_SQL_COMMAND_LOCK_TABLES,
			StoreInLockTables: true, ExternalLock: protocol.ExternalLock_EXTERNAL_LOCK_READ,
			StartStmt: true, StartStmtType: lt, StartStmtSqlCommand: cmd}
	}
	lockWrite := &protocol.ReadLockFacts{StoreLockType: protocol.ThrLockType_TL_WRITE, StoreSqlCommand: protocol.SqlCommand_SQL_COMMAND_LOCK_TABLES,
		StoreInLockTables: true, ExternalLock: protocol.ExternalLock_EXTERNAL_LOCK_WRITE,
		StartStmt: true, StartStmtType: protocol.ThrLockType_TL_WRITE, StartStmtSqlCommand: protocol.SqlCommand_SQL_COMMAND_SELECT}
	cases := []struct {
		name  string
		facts *protocol.ReadLockFacts
		modes [4]protocol.LockMode
	}{
		{"LOCK TABLES READ; SELECT", lockRead(protocol.SqlCommand_SQL_COMMAND_SELECT, protocol.ThrLockType_TL_READ), [4]protocol.LockMode{none, none, none, shared}},
		{"LOCK TABLES READ; INSERT ... SELECT", lockRead(protocol.SqlCommand_SQL_COMMAND_INSERT_SELECT, protocol.ThrLockType_TL_READ), [4]protocol.LockMode{none, none, shared, shared}},
		{"LOCK TABLES READ; DELETE ... IN (SELECT)", lockRead(protocol.SqlCommand_SQL_COMMAND_DELETE, protocol.ThrLockType_TL_READ), [4]protocol.LockMode{shared, shared, shared, shared}},
		{"LOCK TABLES WRITE; SELECT", lockWrite, [4]protocol.LockMode{excl, excl, excl, excl}},
		{"start_stmt without external_lock", &protocol.ReadLockFacts{StartStmt: true, StartStmtType: protocol.ThrLockType_TL_READ,
			StartStmtSqlCommand: protocol.SqlCommand_SQL_COMMAND_SELECT}, [4]protocol.LockMode{excl, excl, excl, excl}},
	}
	for _, c := range cases {
		for i, level := range []protocol.Isolation{ru, rc, rr, ser} {
			got := Isolation{Level: level}.ReadLock(c.facts)
			if got != c.modes[i] {
				t.Errorf("%s at %v: %v, want %v", c.name, level, got, c.modes[i])
			}
		}
	}
}
