package sqliteha_test

import (
	"net/url"
	"testing"

	"github.com/litesql/go-ha/hatest"
	sqliteha "github.com/litesql/go-sqlite3-wasm-ha"
	"github.com/ncruces/go-sqlite3/vfs/mvcc"
)

func TestDriverSuite(t *testing.T) {
	hatest.RunDriverSuite(t, sqliteha.NewConnector, func(t *testing.T, _ string) string {
		return mvcc.TestDB(t, mvcc.NewSnapshot(""), url.Values{
			"_pragma": {"busy_timeout(1000)"},
		})
	})
}

func TestResilienceSuite(t *testing.T) {
	hatest.RunResilienceSuite(t, "sqlite3-wasm-ha")
}
