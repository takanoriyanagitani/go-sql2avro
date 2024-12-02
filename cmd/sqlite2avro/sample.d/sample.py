from typing import Iterator
from typing import Tuple
from typing import Callable

import sqlite3

def filename2rows2sqlite(filename: str)->Callable[[Iterator[dict]], Callable[[], Tuple[()]]]:
	def rows2sqlite(rows: Iterator[dict])->Callable[[], Tuple[()]]:
		def ret()->Tuple[()]:
			with sqlite3.connect(filename) as con:
				con.execute('''
					DROP TABLE IF EXISTS tab1
				''')
				con.execute('''
					CREATE TABLE IF NOT EXISTS tab1(
						id TEXT,
						msg TEXT,
						flag INTEGER,
						amount INTEGER,
						temp INTEGER,
						price REAL
					)
				''')

				con.executemany(
					'''
						INSERT INTO tab1
						VALUES(?,?,?,?,?,?)
					''',
					map(lambda d: (
						d["id"],
						d["msg"],
						d["flag"],
						d["amount"],
						d["temp"],
						d["price"],
					), rows),
				)
				pass
			return ()
		return ret
	return rows2sqlite

rows2sqlite: Callable[[Iterator[dict]], Callable[[], Tuple[()]]] = filename2rows2sqlite("./sample.sqlite.db")

def sampleRows()->Iterator[dict]:
	return iter([
		dict(
			id="helo",
			msg="wrld",
			flag=1,
			amount=299792458,
			temp=-273,
			price=3.776,
		),
		dict(
			id="hl",
			msg="wd",
			flag=0,
			amount=634,
			temp=-273,
			price=42.195,
		),
		dict(
			id="nn",
			msg=None,
			flag=None,
			amount=None,
			temp=None,
			price=None,
		),
	])

rows: Callable[[], Iterator[dict]] = sampleRows

def bind(io, f):
	def ret():
		t = io()
		return f(t)()
	return ret

sampleRows2sqlite: Callable[[], Tuple[()]] = bind(rows, rows2sqlite)

sampleRows2sqlite()
