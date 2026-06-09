# go-sqlite3-wasm-ha

Go database/sql driver based on CGO FREE [github.com/ncruces/go-sqlite3](https://github.com/ncruces/go-sqlite3), providing high availability for SQLite databases.

## Features

- Built on the robust foundation of `github.com/ncruces/go-sqlite3`.
- High availability support for SQLite databases.
- Replication: Synchronize data across nodes using NATS.
- Customize the replication strategy
- Leaderless clusters: Read/Write from/to any node. **Last-writer wins** by default, but you can customize conflict resolutions by implementing *ChangeSetInterceptor*.
- Embedded or External NATS: Choose between an embedded NATS server or an external one for replication.
- Easy to integrate with existing Go projects.

## Installation

```bash
go get github.com/litesql/go-sqlite3-wasm-ha
```

## Usage

```go
package main

import (
    "database/sql"
    _ "github.com/litesql/go-sqlite3-wasm-ha"
)

func main() {
    db, err := sql.Open("sqlite3-wasm-ha", "file:example.db?_journal=WAL&_timeout=5000&replicationURL=nats://broker:4222&name=node0")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Use db to interact with your database
}
```

### Using Connector

```go
package main

import (
    "database/sql"
    sqliteha "github.com/litesql/go-sqlite3-wasm-ha"
)

func main() {
    c, err := sqliteha.NewConnector("file:example.db?_journal=WAL&_timeout=5000",
		ha.WithName("node1"),
		ha.WithEmbeddedNatsConfig(&ha.EmbeddedNatsConfig{
			Port: 4222,
		}))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

    // Use db to interact with your database
}
```

## Configuration

[See options available](https://github.com/litesql/go-ha?tab=readme-ov-file#configuration-options)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.