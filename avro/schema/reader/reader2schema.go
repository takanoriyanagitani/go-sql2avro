package reader2avsc

import (
	"context"
	"io"
	"strings"

	util "github.com/takanoriyanagitani/go-sql2avro/util"
)

const AvroSchemaSizeLimitDefault int64 = 1048576

func ReaderToSchemaLimited(limit int64) func(io.Reader) util.IO[string] {
	return func(r io.Reader) util.IO[string] {
		return func(_ context.Context) (string, error) {
			var buf strings.Builder
			_, e := io.Copy(&buf, r)
			return buf.String(), e
		}
	}
}

func ReaderToSchemaDefault(r io.Reader) util.IO[string] {
	return ReaderToSchemaLimited(AvroSchemaSizeLimitDefault)(r)
}
