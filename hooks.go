package sqliteha

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"

	"github.com/litesql/go-ha"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

type execQuerier interface {
	ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error)
	PrepareContext(ctx context.Context, query string) (driver.Stmt, error)
}

type SQLiteConn interface {
	Raw() *sqlite3.Conn
	execQuerier
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
}

type wasmConn struct {
	SQLiteConn
}

func (c wasmConn) Deserialize(b []byte, _ string) error {
	return fmt.Errorf("Not implemented")
}

func (c wasmConn) UnwrapSQLiteConn() driver.Conn {
	return c.SQLiteConn
}

type hooksAdapter struct{}

func (a *hooksAdapter) WrapConn(c driver.Conn, _ *ha.Connector) (ha.SQLiteConn, error) {
	sqliteConn, ok := c.(SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("connection does not implement SQLiteConn: %T", c)
	}
	return wasmConn{SQLiteConn: sqliteConn}, nil
}

func (a *hooksAdapter) EnableHooks(conn ha.SQLiteConn, cb ha.HookCallbacks) {
	sconn := conn.(wasmConn).SQLiteConn
	sconn.Raw().PreUpdateHook(func(d sqlite3.PreUpdateData) {
		if d.Table == ha.TwoPhaseCommitDecisionTable || d.Table == ha.OutboxTable {
			return
		}
		if change, ok := getChange(&d); ok {
			cb.OnPreUpdate(change)
		}
	})
	sconn.Raw().CommitHook(func() bool {
		return cb.OnCommit() == nil
	})
	sconn.Raw().RollbackHook(cb.OnRollback)
}

func (a *hooksAdapter) DisableHooks(conn ha.SQLiteConn) {
	sconn := conn.(wasmConn).SQLiteConn
	sconn.Raw().PreUpdateHook(nil)
	sconn.Raw().CommitHook(nil)
	sconn.Raw().RollbackHook(nil)
}

func getChange(d *sqlite3.PreUpdateData) (c ha.Change, ok bool) {
	ok = true
	c = ha.Change{
		Database: d.Schema,
		Table:    d.Table,
		OldRowID: d.OldRowID,
		NewRowID: d.NewRowID,
	}
	count := d.Count()
	switch d.Op {
	case sqlite3.AUTH_UPDATE:
		c.Operation = "UPDATE"
		c.OldValues = make([]any, count)
		c.NewValues = make([]any, count)
		for i := range count {
			oldValue, err := d.Old(i)
			if err != nil {
				slog.Error("failed to get preUpdateChange old value", "table", d.Table, "index", i, "error", err)
				continue
			}
			c.OldValues[i] = oldValue
			newValue, err := d.New(i)
			if err != nil {
				slog.Error("failed to get preUpdateChange new value", "table", d.Table, "index", i, "error", err)
				continue
			}
			c.NewValues[i] = newValue
		}
	case sqlite3.AUTH_INSERT:
		c.Operation = "INSERT"
		c.NewValues = make([]any, count)
		for i := range count {
			v, err := d.New(i)
			if err != nil {
				slog.Error("failed to get preUpdateChange new value", "table", d.Table, "index", i, "error", err)
				continue
			}
			c.NewValues[i] = v.Text()
		}
	case sqlite3.AUTH_DELETE:
		c.Operation = "DELETE"
		c.OldValues = make([]any, count)
		for i := range count {
			v, err := d.Old(i)
			if err != nil {
				slog.Error("failed to get preUpdateChange old value", "table", d.Table, "index", i, "error", err)
				continue
			}
			c.OldValues[i] = v.Text()
		}
	default:
		c.Operation = fmt.Sprintf("UNKNOWN - %d", d.Op)
	}

	return
}
