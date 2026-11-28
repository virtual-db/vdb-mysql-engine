// Package innodb — readlock.go
//
// The lock mode of a read, by InnoDB's select_lock_type rule
// (ha_innobase::store_lock, external_lock, start_stmt and
// init_table_handle_for_HANDLER in storage/innobase/handler/ha_innodb.cc,
// MySQL 8.4). vdb.so relays the raw values mysqld passes to those calls
// (protocol.ReadLockFacts) and decides nothing; this file replays InnoDB's
// three functions on them in call order. Handoff #18 and its review #20 hold
// the measurements on stock MySQL 8.4.13 this rule reproduces.
package innodb

import "github.com/virtual-db/vdb-mysql-engine/protocol"

// knownLockType reports whether t is a thr_lock_type value this rule knows.
func knownLockType(t protocol.ThrLockType) bool {
	return t >= protocol.ThrLockType_TL_IGNORE && t <= protocol.ThrLockType_TL_WRITE_ONLY
}

// consistentAtLowIsolation reports whether InnoDB keeps a TL_READ or
// TL_READ_NO_INSERT read of command cmd a consistent read at READ COMMITTED
// and READ UNCOMMITTED (trx->skip_gap_locks()): INSERT ... SELECT,
// REPLACE ... SELECT, UPDATE ... (SELECT ...) and CREATE ... SELECT.
func consistentAtLowIsolation(lt protocol.ThrLockType, cmd protocol.SqlCommand) bool {
	if lt != protocol.ThrLockType_TL_READ && lt != protocol.ThrLockType_TL_READ_NO_INSERT {
		return false
	}
	switch cmd {
	case protocol.SqlCommand_SQL_COMMAND_INSERT_SELECT, protocol.SqlCommand_SQL_COMMAND_REPLACE_SELECT,
		protocol.SqlCommand_SQL_COMMAND_UPDATE, protocol.SqlCommand_SQL_COMMAND_CREATE_TABLE:
		return true
	}
	return false
}

// ReadLock returns the lock mode of a read through a table handle whose
// mysqld calls f records, at isolation i. Unknown or missing values never
// give a snapshot read: InnoDB locks every read of a command other than a
// plain SELECT, so an unknown command or lock type is a shared locking read.
func (i Isolation) ReadLock(f *protocol.ReadLockFacts) protocol.LockMode {
	const (
		none   = protocol.LockMode_LOCK_NONE
		shared = protocol.LockMode_LOCK_SHARED
		excl   = protocol.LockMode_LOCK_EXCLUSIVE
	)
	if f == nil {
		f = &protocol.ReadLockFacts{}
	}
	serializable := i.Level == protocol.Isolation_SERIALIZABLE
	skipGaps := !i.Gaps()

	// store_lock. vdb.so keeps the last call whose type was not TL_IGNORE,
	// as InnoDB keeps its state on TL_IGNORE; no call seen at all is the
	// unknown case.
	stored := shared
	lt, cmd := f.StoreLockType, f.StoreSqlCommand
	switch {
	case !knownLockType(lt) || lt == protocol.ThrLockType_TL_IGNORE:
		// Unknown: a locking read.
	case cmd == protocol.SqlCommand_SQL_COMMAND_FLUSH && lt == protocol.ThrLockType_TL_READ_NO_INSERT:
		// FLUSH TABLES ... WITH READ LOCK / FOR EXPORT.
		stored = none
		if serializable {
			stored = shared
		}
	case cmd == protocol.SqlCommand_SQL_COMMAND_DROP_TABLE:
		// InnoDB leaves the handle's state alone; DROP TABLE reads no rows.
	case (lt == protocol.ThrLockType_TL_READ && f.StoreInLockTables) ||
		(lt == protocol.ThrLockType_TL_READ_HIGH_PRIORITY && f.StoreInLockTables) ||
		lt == protocol.ThrLockType_TL_READ_WITH_SHARED_LOCKS ||
		lt == protocol.ThrLockType_TL_READ_NO_INSERT ||
		cmd != protocol.SqlCommand_SQL_COMMAND_SELECT:
		// LOCK TABLES ... READ, FOR SHARE, and every read of a command
		// other than a plain SELECT (unknown commands included), except a
		// checksum and the low-isolation exception.
		if cmd == protocol.SqlCommand_SQL_COMMAND_CHECKSUM || (skipGaps && consistentAtLowIsolation(lt, cmd)) {
			stored = none
		}
	default:
		// A plain SELECT; FOR UPDATE becomes exclusive in external_lock.
		stored = none
	}

	// external_lock.
	switch f.ExternalLock {
	case protocol.ExternalLock_EXTERNAL_LOCK_WRITE:
		stored = excl
	case protocol.ExternalLock_EXTERNAL_LOCK_READ:
		if serializable && stored == none && !i.Autocommit {
			stored = shared
		}
	}
	mode := stored

	// start_stmt: each statement under LOCK TABLES.
	if f.StartStmt {
		st, scmd := f.StartStmtType, f.StartStmtSqlCommand
		switch {
		case f.ExternalLock == protocol.ExternalLock_EXTERNAL_LOCK_UNSPECIFIED:
			// No external_lock (a table created inside this LOCK TABLES).
			mode = excl
		case (!serializable && st == protocol.ThrLockType_TL_READ && scmd == protocol.SqlCommand_SQL_COMMAND_SELECT) ||
			(skipGaps && consistentAtLowIsolation(st, scmd)):
			mode = none
		default:
			mode = stored
		}
	}

	// HANDLER ... READ: a consistent read.
	if f.ForHandler {
		mode = none
	}
	return mode
}
