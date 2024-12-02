package maps2avro

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	util "github.com/takanoriyanagitani/go-sql2avro/util"
)

func MapsToWriter(
	ctx context.Context,
	s ha.Schema,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
) error {
	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		if nil != e {
			return e
		}
		var ee error = enc.Encode(row)
		if nil != ee {
			return ee
		}
	}
	return nil
}

func SchemaStringToMapsToWriter(
	ctx context.Context,
	schema string,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
) error {
	s, e := ha.Parse(schema)
	if nil != e {
		return e
	}
	return MapsToWriter(ctx, s, m, w)
}

func SchemaStringMapsToStdout(
	ctx context.Context,
	schema string,
	m iter.Seq2[map[string]any, error],
) error {
	return SchemaStringToMapsToWriter(ctx, schema, m, os.Stdout)
}

func MapsToStdoutFromSchema(
	schema string,
) func(iter.Seq2[map[string]any, error]) util.IO[util.Void] {
	return func(m iter.Seq2[map[string]any, error]) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			return util.Empty, SchemaStringMapsToStdout(
				ctx,
				schema,
				m,
			)
		}
	}
}
