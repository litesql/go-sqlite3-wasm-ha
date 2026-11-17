package sqliteha_test

import (
	"context"
	"database/sql"
	"net/url"
	"reflect"
	"testing"

	"github.com/litesql/go-ha"
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

type capturePublisher struct {
	changes []ha.Change
}

func (p *capturePublisher) Publish(_ context.Context, cs *ha.ChangeSet) error {
	p.changes = append([]ha.Change(nil), cs.Changes...)
	return nil
}

func (p *capturePublisher) Sequence() uint64 { return 0 }

func TestPreUpdateHookValueTypes(t *testing.T) {
	pub := &capturePublisher{}
	dsn := mvcc.TestDB(t, mvcc.NewSnapshot(""), url.Values{
		"_pragma": {"busy_timeout(1000)"},
	})
	connector, err := sqliteha.NewConnector(dsn, ha.WithReplicationPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE items(id INTEGER PRIMARY KEY, score REAL, label TEXT, payload BLOB, note TEXT)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO items(id, score, label, payload, note) VALUES(?, ?, ?, ?, ?)`,
		int64(7), 3.5, "alpha", []byte{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	wantInsert := []any{int64(7), 3.5, "alpha", []byte{1, 2, 3}, nil}
	if len(pub.changes) != 1 || !reflect.DeepEqual(pub.changes[0].NewValues, wantInsert) {
		t.Fatalf("INSERT NewValues = %#v, want %#v", pub.changes[0].NewValues, wantInsert)
	}

	if _, err := db.ExecContext(ctx, `UPDATE items SET score = ?, label = ?, payload = ?, note = ? WHERE id = ?`,
		9.25, "beta", []byte{}, "updated", int64(7)); err != nil {
		t.Fatal(err)
	}
	wantUpdateNew := []any{int64(7), 9.25, "beta", []byte{}, "updated"}
	if len(pub.changes) != 1 {
		t.Fatalf("expected 1 UPDATE change, got %d", len(pub.changes))
	}
	if !reflect.DeepEqual(pub.changes[0].OldValues, wantInsert) {
		t.Fatalf("UPDATE OldValues = %#v, want %#v", pub.changes[0].OldValues, wantInsert)
	}
	if !reflect.DeepEqual(pub.changes[0].NewValues, wantUpdateNew) {
		t.Fatalf("UPDATE NewValues = %#v, want %#v", pub.changes[0].NewValues, wantUpdateNew)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM items WHERE id = ?`, int64(7)); err != nil {
		t.Fatal(err)
	}
	if len(pub.changes) != 1 || !reflect.DeepEqual(pub.changes[0].OldValues, wantUpdateNew) {
		t.Fatalf("DELETE OldValues = %#v, want %#v", pub.changes[0].OldValues, wantUpdateNew)
	}
}
