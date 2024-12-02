package sql2avro

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
)

var (
	ErrUnexpectedInputValue error = errors.New("unexpected input value")
)

type AnyColumn struct {
	Value       any
	Name        string
	RawTypeName string
}

func (a *AnyColumn) Scan(src any) error {
	a.Value = src
	return nil
}

func (a AnyColumn) String() string {
	return fmt.Sprintf(
		"name: %s, db type name: %s, value: %v",
		a.Name,
		a.RawTypeName,
		a.Value,
	)
}

type AnyRow []*AnyColumn

func (a AnyRow) ToMap(m map[string]any) {
	for _, col := range a {
		var name string = col.Name
		m[name] = col.Value
	}
}

// Converts the [*sql.Rows] to an iterator and closes it.
func SqlRowsToMaps(r *sql.Rows) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		defer r.Close()

		types, e := r.ColumnTypes()
		if nil != e {
			yield(nil, e)
			return
		}

		row := map[string]any{}

		var buf []any = make([]any, 0, len(types))
		var cols []*AnyColumn = make([]*AnyColumn, 0, len(types))

		for _, colTyp := range types {
			col := AnyColumn{
				Name:        colTyp.Name(),
				RawTypeName: colTyp.DatabaseTypeName(),
			}
			cols = append(cols, &col)
			buf = append(buf, &col)
		}

		for r.Next() {
			e := r.Scan(buf...)

			clear(row)
			if nil == e {
				AnyRow(cols).ToMap(row)
			}

			if !yield(row, e) {
				return
			}
		}
	}
}
