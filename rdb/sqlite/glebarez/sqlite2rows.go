package sqlite2rows

import (
	"context"
	"database/sql"
	"iter"
	"log"

	_ "github.com/glebarez/go-sqlite"

	sa "github.com/takanoriyanagitani/go-sql2avro"
)

func Filename2Maps(
	ctx context.Context,
	filename string,
	trustedQuery string,
) iter.Seq2[map[string]any, error] {
	db, de := sql.Open("sqlite", filename)

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
