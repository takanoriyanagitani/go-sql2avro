package fs2avsc

import (
	"context"
	"os"

	util "github.com/takanoriyanagitani/go-sql2avro/util"

	sr "github.com/takanoriyanagitani/go-sql2avro/avro/schema/reader"
)

func FilenameToSchemaLimited(limit int64) func(string) util.IO[string] {
	return func(filename string) util.IO[string] {
		return func(ctx context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()
			return sr.ReaderToSchemaLimited(limit)(f)(ctx)
		}
	}
}

func FilenameToSchemaDefault(filename string) util.IO[string] {
	return FilenameToSchemaLimited(sr.AvroSchemaSizeLimitDefault)(filename)
}
