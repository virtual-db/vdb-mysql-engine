// Package source_test — tests/log_test.go
//
// The request log keeps the last size records, oldest first, and allocates
// only for the records it holds (vdb-mysql-engine#12).
package source_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/virtual-db/vdb-mysql-engine/internal/source"
)

func TestLogKeepsLastRecords(t *testing.T) {
	for _, n := range []int{0, 1, 5, 6, 7, 130, 1000} {
		l := source.NewLog(6, "", nil)
		for i := 0; i < n; i++ {
			l.Add(source.Record{ConnID: uint64(i)})
		}
		var got []uint64
		for _, r := range l.Records() {
			got = append(got, r.ConnID)
		}
		var want []uint64
		for i := max(0, n-6); i < n; i++ {
			want = append(want, uint64(i))
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("after %d records: %v, want %v", n, got, want)
		}
		if l.Count.Load() != int64(n) {
			t.Fatalf("count %d, want %d", l.Count.Load(), n)
		}
	}
}

// TestLogAllocatesOnUse: a log sized for 3,000,000 records (408 MB) costs
// almost nothing until records arrive.
func TestLogAllocatesOnUse(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	l := source.NewLog(3_000_000, "", nil)
	for i := 0; i < 100; i++ {
		l.Add(source.Record{ConnID: uint64(i)})
	}
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Fatalf("a 3,000,000-record log with 100 records allocated %d bytes", grew)
	}
	runtime.KeepAlive(l)
}
