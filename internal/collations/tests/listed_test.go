// Package collations_test — tests/listed_test.go
//
// Listed reports whether proven.yaml has an entry for a mysqld release
// (vdb-mysql#23). The releases here are listed in no proven.yaml; the image
// build in vdb-mysql checks the listed case on the real release.
package collations_test

import (
	"testing"

	"github.com/virtual-db/vdb-mysql-engine/internal/collations"
)

func TestListedUnknownRelease(t *testing.T) {
	for _, release := range []string{"8.4.12", "9.1.0", "0.0.0-none", ""} {
		if collations.Listed(release) {
			t.Fatalf("Listed(%q) = true; proven.yaml has no entry for it", release)
		}
	}
}
