package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
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

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	dsn, opts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, d.Options...)
	drv := new(sqlite3.SQLite)
	return ha.NewConnector(dsn, drv, func(cfg ha.ConnHooksConfig) ha.ConnHooksProvider {
		return newConnHooksProvider(cfg)
	}, Backup, opts...)
}

func NewConnector(name string, opts ...ha.Option) (*ha.Connector, error) {
	dsn, nameOpts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, nameOpts...)
	drv := new(sqlite3.SQLite)
	return ha.NewConnector(dsn, drv, func(cfg ha.ConnHooksConfig) ha.ConnHooksProvider {
		return newConnHooksProvider(cfg)
	}, Backup, opts...)

}
