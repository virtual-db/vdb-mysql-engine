// Package autoinc hands out auto-increment values as InnoDB does
// (implementation plan doc 09 §3.5, product doc 10 §5, D-15): each table's
// counter is seeded once from max(Source AUTO_INCREMENT, highest committed
// Delta value) plus the gap, then counted up. Values are never handed out
// twice and are not returned on rollback. Counters are not persisted: after a
// restart they seed again, which the gap allows.
package autoinc

import (
	"github.com/virtual-db/vdb-mysql-engine/internal/mysqldef"
	"strconv"
	"sync"

	core "github.com/virtual-db/vdb-core"
)

// DefaultGap is the gap above the Source's counter (D-15).
const DefaultGap = 1000000

// Counters holds every table's counter. Safe for concurrent use.
type Counters struct {
	api core.DriverAPI
	gap uint64

	mu sync.Mutex
	m  map[core.TableID]*counter
}

type counter struct {
	mu     sync.Mutex
	next   uint64
	seeded bool
}

// New returns empty counters; gap 0 means DefaultGap.
func New(api core.DriverAPI, gap uint64) *Counters {
	if gap == 0 {
		gap = DefaultGap
	}
	return &Counters{api: api, gap: gap, m: map[core.TableID]*counter{}}
}

// Next hands out n values for table t, aligned to offset + k*increment when
// those are above 1 (auto_increment_offset and auto_increment_increment).
func (c *Counters) Next(t core.TableID, n, offset, increment uint64) (first, granted uint64, err error) {
	if n == 0 {
		n = 1
	}
	if increment == 0 {
		increment = 1
	}
	c.mu.Lock()
	ct := c.m[t]
	if ct == nil {
		ct = &counter{}
		c.m[t] = ct
	}
	c.mu.Unlock()
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if !ct.seeded {
		def, ok := c.api.Catalog().Table(t)
		if !ok {
			return 0, 0, core.Errorf(core.ErrInternal, "auto-increment on unknown table")
		}
		next := c.highest(def) + 1
		if def.Kind == core.Mirrored && def.Source != nil {
			src := mysqldef.Src(&def).AutoInc
			if src < 1 {
				src = 1
			}
			if src > next {
				next = src
			}
			next += c.gap
		}
		ct.next, ct.seeded = next, true
	}
	first = ct.next
	if offset > 1 || increment > 1 {
		if offset == 0 {
			offset = 1
		}
		if first < offset {
			first = offset
		} else if r := (first - offset) % increment; r != 0 {
			first += increment - r
		}
	}
	ct.next = first + n*increment
	return first, n, nil
}

// highest is the largest committed Delta value of the table's
// auto-increment column.
func (c *Counters) highest(def core.TableDef) uint64 {
	col := -1
	for i, cd := range def.Columns {
		if mysqldef.ColOf(cd).AutoIncrement {
			col = i
			break
		}
	}
	if col < 0 {
		return 0
	}
	var high uint64
	c.api.Delta().Rows(def.ID, func(r core.DeltaRowInfo) bool {
		if col < len(r.Row) && !r.Row[col].Null {
			if v, err := strconv.ParseUint(string(r.Row[col].B), 10, 64); err == nil && v > high {
				high = v
			}
		}
		return true
	})
	return high
}
