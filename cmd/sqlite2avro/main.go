package main

import (
	"context"
	"fmt"
	"iter"
	"log"
	"os"

	util "github.com/takanoriyanagitani/go-sql2avro/util"

	sq "github.com/takanoriyanagitani/go-sql2avro/common/sql/query"
	sf "github.com/takanoriyanagitani/go-sql2avro/avro/schema/fs"

	sg "github.com/takanoriyanagitani/go-sql2avro/rdb/sqlite/glebarez"

	ah "github.com/takanoriyanagitani/go-sql2avro/avro/hamba"
)

func GetEnvByKey(key string) util.IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("no env var defined: %s", key)
		}
	}
}

var sqlite2maps func(
	ctx context.Context,
	filename string,
	trustedQuery string,
) iter.Seq2[map[string]any, error] = sg.Filename2Maps

var sqliteFilename util.IO[string] = GetEnvByKey("ENV_SQLITE_DB_FILENAME")

var stdinToSql util.IO[string] = sq.Reader2sqlDefault(os.Stdin)

var anyMaps util.IO[iter.Seq2[map[string]any, error]] = util.Bind(
	sqliteFilename,
	func(filename string) util.IO[iter.Seq2[map[string]any, error]] {
		return func(
			ctx context.Context,
		) (iter.Seq2[map[string]any, error], error) {
			trustedQuery, e := stdinToSql(ctx)
			if nil != e {
				return nil, e
			}
			return sqlite2maps(ctx, filename, trustedQuery), nil
		}
	},
)

var schemaFilenameToStringLimitedDefault func(
	filename string,
) util.IO[string] = sf.FilenameToSchemaDefault

var schemaFilename util.IO[string] = GetEnvByKey("ENV_SCHEMA_FILENAME")

var schemaContent util.IO[string] = util.Bind(
	schemaFilename,
	schemaFilenameToStringLimitedDefault,
)

var sqlite2maps2avro util.IO[util.Void] = util.Bind(
	anyMaps,
	func(m iter.Seq2[map[string]any, error]) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			schema, e := schemaContent(ctx)
			if nil != e {
				return util.Empty, e
			}

			var maps2stdout func(
				iter.Seq2[map[string]any, error],
			) util.IO[util.Void] = ah.MapsToStdoutFromSchema(schema)

			return maps2stdout(m)(ctx)
		}
	},
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, e := sqlite2maps2avro(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
