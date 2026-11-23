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

// TestProven8413 checks the 8.4.13 entry (vdb-tests#16): the nine collations
// are proven on 8.4.13 and on no other release.
func TestProven8413(t *testing.T) {
	p := collations.For("8.4.13")
	for _, c := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_0900_as_cs", "utf8mb4_0900_as_ci", "utf8mb4_general_ci",
		"utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci", "utf8mb3_general_ci", "latin1_swedish_ci", "ascii_general_ci"} {
		if !p.Order(c) || !p.Equality(c) {
			t.Fatalf("%s is not proven on 8.4.13", c)
		}
	}
	if p.Order("utf8mb4_croatian_ci") {
		t.Fatal("a collation the suite did not prove must stay unproven")
	}
	if collations.For("8.4.11").Order("utf8mb4_0900_ai_ci") {
		t.Fatal("8.4.13's entry must not prove another release")
	}
}

func TestListed(t *testing.T) {
	for release, want := range map[string]bool{"8.4.13": true, "8.4.12": false, "8.4": false, "9.1.0": false, "": false} {
		if got := collations.Listed(release); got != want {
			t.Fatalf("Listed(%q) = %v, want %v", release, got, want)
		}
	}
}
