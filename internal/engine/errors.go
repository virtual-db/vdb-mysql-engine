// Package engine — errors.go
//
// core.Error and fk.Error to protocol errors (doc 07 §4, doc 09 §3.4). The C++ maps each code to its
// HA_ERR_* value in one table (vdb-mysql-plugins/src/common/errors.cc).
package engine

import (
	"errors"

	core "github.com/virtual-db/vdb-core"

	"github.com/virtual-db/vdb-mysql-engine/internal/fk"
	"github.com/virtual-db/vdb-mysql-engine/protocol"
)

var codes = map[core.ErrorCode]uint32{
	core.ErrDuplicateKey:    protocol.CodeDuplicateKey,
	core.ErrLockWaitTimeout: protocol.CodeLockWaitTimeout,
	core.ErrDeadlock:        protocol.CodeDeadlock,
	core.ErrDeltaFull:       protocol.CodeDeltaFull,
	core.ErrLockMemory:      protocol.CodeLockMemory,
	core.ErrRefused:         protocol.CodeRefused,
	core.ErrSource:          protocol.CodeSource,
	core.ErrKilled:          protocol.CodeKilled,
	core.ErrInternal:        protocol.CodeInternal,
}

var fkCodes = map[fk.Code]uint32{
	fk.NoReferencedRow: protocol.CodeNoReferencedRow,
	fk.RowIsReferenced: protocol.CodeRowIsReferenced,
	fk.Depth:           protocol.CodeFKDepth,
}

// toError turns any error into a protocol error. Source errors keep their
// retryable message naming the table.
func toError(err error) *protocol.Error {
	var fe *fk.Error
	if errors.As(err, &fe) {
		return &protocol.Error{Code: fkCodes[fe.Code], Constraint: fe.Constraint, Message: fe.Msg}
	}
	var ce *core.Error
	if errors.As(err, &ce) {
		code, ok := codes[ce.Code]
		if !ok {
			code = protocol.CodeInternal
		}
		return &protocol.Error{Code: code, Index: uint32(ce.Index), Holder: ce.Holder, Message: ce.Msg}
	}
	return &protocol.Error{Code: protocol.CodeInternal, Message: err.Error()}
}
