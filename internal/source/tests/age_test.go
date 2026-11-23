// Package source_test — tests/age_test.go
//
// The snapshot's maximum age is checked between statements only (Source
// scaling plan S2, SD-2).
package source_test

import (
	"strings"
	"testing"
	"time"

	. "github.com/virtual-db/vdb-mysql-engine/internal/source"
)

func TestAgeCheckedOnlyBetweenStatements(t *testing.T) {
	max := 15 * time.Minute
	opened := time.Now()
	var checked uint64

	// Statement 1 starts young, then reads for longer than the maximum age:
	// its later requests are not checked, so it completes.
	if AgeExpired(1, &checked, opened, opened.Add(time.Second), max) {
		t.Fatal("a young snapshot is not expired")
	}
	for _, at := range []time.Duration{10 * time.Minute, 20 * time.Minute, 2 * time.Hour} {
		if AgeExpired(1, &checked, opened, opened.Add(at), max) {
			t.Fatalf("statement 1 still reading at %v keeps its snapshot", at)
		}
	}
	// The transaction's next statement finds the snapshot too old.
	if !AgeExpired(2, &checked, opened, opened.Add(2*time.Hour), max) {
		t.Fatal("the next statement of an old transaction fails at its first request")
	}
	// Without statement counts every request is checked, as before.
	checked = 0
	if !AgeExpired(0, &checked, opened, opened.Add(20*time.Minute), max) {
		t.Fatal("without a statement count, every request is checked")
	}
	if msg := ErrTooOld(max).Error(); !strings.Contains(msg, "VDB_SOURCE_SNAPSHOT_MAX_AGE") {
		t.Fatalf("the error names the setting: %s", msg)
	}
}
