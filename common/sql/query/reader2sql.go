package query

import (
	"context"
	"io"
	"strings"

	util "github.com/takanoriyanagitani/go-sql2avro/util"
)

const SqlQueryLengthLimitDefault int64 = 1048576

func Reader2sql(limit int64, r io.Reader) util.IO[string] {
	return func(_ context.Context) (string, error) {
		var limited io.Reader = &io.LimitedReader{
			R: r,
			N: limit,
		}
		var buf strings.Builder
		_, e := io.Copy(&buf, limited)
		return buf.String(), e
	}
}

func Reader2sqlDefault(r io.Reader) util.IO[string] {
	return Reader2sql(SqlQueryLengthLimitDefault, r)
}
