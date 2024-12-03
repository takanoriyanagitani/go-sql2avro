//go:build pgx
// +build pgx

package pg2rows

import (
	"context"
	"database/sql"
	"iter"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"

	sa "github.com/takanoriyanagitani/go-sql2avro"
)

func ConnString2Maps(
	ctx context.Context,
	connString string,
	trustedQuery string,
) iter.Seq2[map[string]any, error] {
	db, de := sql.Open("pgx", connString)

	if nil != de {
		return func(yield func(map[string]any, error) bool) {
			yield(nil, de)
		}
	}

	go func() {
		<-ctx.Done()

		e := db.Close()
		if nil != e {
			log.Printf("db close error: %v\n", e)
		}
	}()

	rows, e := db.QueryContext(ctx, trustedQuery)
	if nil != e {
		return func(yield func(map[string]any, error) bool) {
			yield(nil, e)
		}
	}

	return sa.SqlRowsToMaps(rows)
}
