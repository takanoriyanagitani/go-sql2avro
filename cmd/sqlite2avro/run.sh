#!/bin/sh

export ENV_SQLITE_DB_FILENAME=./sample.d/sample.sqlite.db
export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

echo '
	SELECT * FROM tab1
' |
	./sqlite2avro |
	rq \
		--input-avro \
		--output-json
