package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

type connHooksProvider struct {
	*ha.Connector
}

func (p *connHooksProvider) RegisterHooks(c driver.Conn, connector *ha.Connector) (driver.Conn, error) {
	p.Connector = connector
	sqliteConn, ok := c.(SQLiteConn)
	if !ok {
		slog.Error("connection does not implement SQLiteConn", "type", fmt.Sprintf("%T", c))
	}
	enableCDCHooks(sqliteConn, p.Connector)
	conn := &Conn{
		SQLiteConn:              sqliteConn,
		disableDDLSync:          p.DisableDDLSync(),
		enableRedirect:          true,
		replicationID:           p.ReplicationID(),
		leader:                  p.LeaderProvider(),
		reqCh:                   make(chan *sqlv1.QueryRequest),
		resCh:                   make(chan *sqlv1.QueryResponse),
		txseqTracker:            p.Subscriber(),
		timeout:                 p.GrpcTimeout(),
		token:                   p.GrpcToken(),
		insecure:                p.GrpcInsecure(),
		proxiedDB:               connector.ProxiedDB(),
		proxiedPositionProvider: connector.ProxiedPositionProvider(),
		queryRouter:             p.QueryRouter(),
	}
	return conn, conn.start()
}

func (p *connHooksProvider) DisableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	sconn.Raw().PreUpdateHook(nil)
	sconn.Raw().CommitHook(nil)
	sconn.Raw().RollbackHook(nil)
	sconn.enableRedirect = false
	return nil
}

func (p *connHooksProvider) EnableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	enableCDCHooks(sconn.SQLiteConn, p.Connector)
	sconn.enableRedirect = true
	return sconn.start()
}

type tableSchema struct {
	columns, types, pkColumns []string
}

var tableSchemaCache = make(map[string]map[string]*tableSchema)

func clearTableSchemaCache(replicationID string) {
	delete(tableSchemaCache, replicationID)
}

func getTableSchema(sconn SQLiteConn, replicationID string, database string, table string) (*tableSchema, error) {
	key := fmt.Sprintf("%s.%s", database, table)
	replicationTables, ok := tableSchemaCache[replicationID]
	if !ok {
		replicationTables = map[string]*tableSchema{}
		tableSchemaCache[replicationID] = replicationTables
	} else {
		if schema, ok := replicationTables[key]; ok {
			return schema, nil
		}
	}
	var schema tableSchema
	stmt, err := sconn.PrepareContext(context.Background(), fmt.Sprintf("SELECT name, type, pk FROM %s.PRAGMA_TABLE_INFO('%s') ORDER BY cid", database, table))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rows.Columns()
	for {

		dataRow := []driver.Value{new(string), new(string), new(int64)}
		err := rows.Next(dataRow)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("failed to read table columns", "error", err, "table", table)
			}
			break
		}
		if v, ok := dataRow[0].(string); ok {
			schema.columns = append(schema.columns, v)
		} else {
			continue
		}
		if v, ok := dataRow[1].(string); ok {
			schema.types = append(schema.types, v)
		}
		if v, ok := dataRow[2].(int64); ok && v > 0 {
			schema.pkColumns = append(schema.pkColumns, dataRow[0].(string))
		}
	}
	replicationTables[key] = &schema
	return &schema, nil

}

func enableCDCHooks(sconn SQLiteConn, connector *ha.Connector) {
	cs := ha.NewChangeSet(connector.NodeName(), connector.ReplicationID())
	changeSetSessionsMu.Lock()
	changeSetSessions[sconn] = cs
	changeSetSessionsMu.Unlock()
	sconn.Raw().PreUpdateHook(func(d sqlite3.PreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		schema, err := getTableSchema(sconn, connector.ReplicationID(), change.Database, change.Table)
		if err != nil {
			slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
			return
		}
		change.Columns = schema.columns
		change.PKColumns = schema.pkColumns
		cs.AddChange(change)
	})

	sconn.Raw().CommitHook(func() bool {
		if err := cs.Send(connector.Publisher()); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return false
		}
		if connector.CDCPublisher() != nil {
			data := cs.DebeziumData()
			if len(data) > 0 {
				if err := connector.CDCPublisher().Publish(data); err != nil {
					slog.Error("failed to send cdc", "error", err)
					return false
				}
			}
		}
		return true
	})
	sconn.Raw().RollbackHook(func() {
		cs.Clear()
	})
}

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

var (
	changeSetSessions   = make(map[SQLiteConn]*ha.ChangeSet)
	changeSetSessionsMu sync.RWMutex
)

func addSQLChange(conn SQLiteConn, sql string, args []any) error {
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	cs.AddChange(ha.Change{
		Operation: "SQL",
		Command:   sql,
		Args:      args,
	})
	return nil
}

func removeLastChange(conn SQLiteConn) error {
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(cs.Changes) > 0 {
		cs.Changes = cs.Changes[:len(cs.Changes)-1]
	}
	return nil
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
