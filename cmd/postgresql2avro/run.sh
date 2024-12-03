#!/bin/sh

export ENV_POSTGRESQL_CONN_STR=postgres://postgres@127.0.0.1:5432/postgres
export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

echo "

	SELECT
	  'helo' AS id,
	  'wrld' AS msg,
	  1 AS flag,
	  299792458 AS amount,
	  -273 AS temp,
	  2.99792458::DOUBLE PRECISION AS price,
	  (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())*1e6)::BIGINT AS created

	UNION ALL

	SELECT
	  'hl' AS id,
	  'wr' AS msg,
	  0 AS flag,
	  634 AS amount,
	  -1 AS temp,
	  42.195::DOUBLE PRECISION AS price,
	  (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())*1e6)::BIGINT AS created

" |
	./postgresql2avro |
	rq \
		--input-avro \
		--output-json
