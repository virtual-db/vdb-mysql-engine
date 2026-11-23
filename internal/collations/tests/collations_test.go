// Package collations_test — tests/collations_test.go
//
// Binary collations are always proven; Assume adds collations under test
// (vdb-mysql-engine#11).
package collations_test

import (
	"testing"

	"github.com/virtual-db/vdb-mysql-engine/internal/collations"
)

func TestAssume(t *testing.T) {
	p := collations.For("0.0.0-none")
	if !p.Order("utf8mb4_bin") || !p.Equality("") {
		t.Fatal("binary collations must be proven")
	}
	if p.Order("utf8mb4_0900_ai_ci") || p.Equality("utf8mb4_0900_ai_ci") {
		t.Fatal("a release with no entry proves no other collation")
	}
	p.Assume([]string{"UTF8MB4_0900_AI_CI"})
	if !p.Order("utf8mb4_0900_ai_ci") || !p.Equality("utf8mb4_0900_ai_ci") || p.Order("latin1_swedish_ci") {
		t.Fatal("Assume must prove exactly the collations it names")
	}
}
