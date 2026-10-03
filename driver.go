package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	_ "unsafe"

	"github.com/litesql/go-ha"
	sqlite3 "github.com/ncruces/go-sqlite3/driver"
)

func init() {
	sql.Register("sqlite3-wasm-ha", &Driver{})
}

type Driver struct {
	once    sync.Once
	Options []ha.Option
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func normalizeDSN(dsn string) string {
	if dsn == ":memory:" {
		return "file::memory:?cache=shared"
	}
	if strings.HasPrefix(dsn, "file::memory:") && !strings.Contains(dsn, "cache=shared") {
		if strings.Contains(dsn, "?") {
			return dsn + "&cache=shared"
		}
		return dsn + "?cache=shared"
	}
	return dsn
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	dsn, opts, err := ha.NameToOptions(name, "sqlite3")
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	dsn = normalizeDSN(dsn)
	opts = append(opts, d.Options...)
	drv := new(sqlite3.SQLite)
	return ha.NewConnector(dsn, drv, func() ha.ConnHooksProvider {
		return &connHooksProvider{}
	}, Backup, opts...)
}

func NewConnector(name string, opts ...ha.Option) (*ha.Connector, error) {
	dsn, nameOpts, err := ha.NameToOptions(name, "sqlite3")
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	dsn = normalizeDSN(dsn)
	opts = append(opts, nameOpts...)
	drv := new(sqlite3.SQLite)
	return ha.NewConnector(dsn, drv, func() ha.ConnHooksProvider {
		return &connHooksProvider{}
	}, Backup, opts...)

}
