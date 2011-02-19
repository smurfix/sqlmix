#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import

"""\
Simple test script
"""

import os
from sqlmix import Db
from warnings import filterwarnings

filterwarnings("ignore",category=RuntimeWarning,lineno=15)
dbname = os.tempnam("/tmp","sqlmix")

def run_test():
	db=Db(cfgfile="./test.ini", database=dbname)
	db.Do("""\
		create table test1 (
			id integer primary key not null,
			a varchar(255) not null default '',
			b varchar(255) null
		)
	""")
	db.commit()
	A = db.Do("insert into test1(a,b) values (${a},${b})", a="one",b="OneOne")
	B = db.Do("insert into test1(a) values (${a})", a="two")
	C = db.Do("insert into test1(b) values (${b})", b="three")
	db.commit()
	def chk(db):
		a,b = db.DoFn("select a,b from test1 where id=${id}", id=A)
		assert a=="one"; assert b=="OneOne"
		a,b = db.DoFn("select a,b from test1 where id=${id}", id=B)
		assert a=="two"; assert b is None
		a,b = db.DoFn("select a,b from test1 where id=${id}", id=C)
		assert a==""; assert b=="three"
	chk(db)
	chk(Db(database=dbname, dbtype="sqlite"))
	db.Do("delete from test1 where id=${id}", id=B)
	db.rollback()

	n=0; j=A
	for i, in db.DoSelect("select id from test1 order by id"):
		n += 1
		assert i==j
		j += 1
	assert n == 3
	print "Success."


try:
	run_test()
finally:
	try: os.unlink(dbname)
	except Exception: pass
