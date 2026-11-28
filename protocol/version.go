// Package protocol — version.go
package protocol

// Version is the plug-in protocol version, raised on any incompatible change.
const Version = 2

// MinVersion is the oldest version vdb-mysql accepts. Normally the current and
// the previous one (product doc 04 §1); version 1 is refused because a v1
// vdb.so sends no ReadLockFacts, so every read would silently lock.
const MinVersion = 2

// Accepts reports whether v is an accepted protocol version.
func Accepts(v uint32) bool { return v >= MinVersion && v <= Version && v > 0 }
