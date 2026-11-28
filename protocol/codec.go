// Package protocol — codec.go
//
// Marshalling bodies with the op code and frame of doc 07 §1.
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Encode writes msg as a frame of op.
// The body is marshalled straight after the frame header, so the frame is
// one allocation and one write, with no copy of the body. The size measured
// by proto.Size is reused (UseCachedSize), so msg must not be changed or
// shared with another goroutine while Encode runs; replies are built per
// request.
func Encode(w io.Writer, op Op, msg proto.Message) error {
	if msg == nil {
		return WriteFrame(w, op, nil)
	}
	size := proto.Size(msg)
	if size > MaxBody {
		return ErrTooLarge
	}
	buf, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(make([]byte, 5, 5+size), msg)
	if err != nil {
		return fmt.Errorf("protocol: marshal %s: %w", op, err)
	}
	if len(buf)-5 > MaxBody {
		return ErrTooLarge
	}
	buf[0] = byte(op)
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(buf)-5))
	_, err = w.Write(buf)
	return err
}

// Decode unmarshals a body into msg.
func Decode(body []byte, msg proto.Message) error {
	if err := proto.Unmarshal(body, msg); err != nil {
		return fmt.Errorf("protocol: malformed body: %w", err)
	}
	return nil
}

// Error codes (doc 07 §4). vdb.so maps each to its HA_ERR_* value in one
// table (vdb-mysql-plugins/src/common/errors.cc); the numbers are part of the
// protocol and never change.
const (
	CodeDuplicateKey    = 1
	CodeNoReferencedRow = 2
	CodeRowIsReferenced = 3
	CodeFKDepth         = 4
	CodeLockWaitTimeout = 5
	CodeDeadlock        = 6
	CodeDeltaFull       = 7
	CodeLockMemory      = 8
	CodeRefused         = 9
	CodeSource          = 10
	CodeKilled          = 11
	CodeInternal        = 12
	CodeProtocol        = 1000 // unknown op, malformed body
	CodeVersion         = 1001 // protocol version refused
	CodeUnavailable     = 1002 // vdb-mysql cannot serve the request
)
