#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import,print_function

"""\
Simple test script
"""

import os
from sqlmix import Db,ManyData,NoData
from warnings import filterwarnings

filterwarnings("ignore",category=RuntimeWarning,lineno=15)

def run_test(x,ai,*a):
	db = Db("db"+str(x),config="test.ini")
	for c in a:
		db.Do(c,_empty=True)
	db.Do("""\
		create table test1 (
			id integer primary key not null %s,
			a varchar(255) not null default '',
			b varchar(255) null
		)
	""" % (ai,), _empty=True)
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
	db.Do("delete from test1 where id=${id}", id=B)
	db.rollback()

	n=0; j=A
	for i, in db.DoSelect("select id from test1 order by id"):
		n += 1
		assert i==j
		j += 1
	assert n == 3
	try:
		db.DoFn("select id from test1")
	except ManyData:
		pass
	else:
		assert False,"Need to raise ManyData"

	try:
		for i, in db.DoSelect("select id from test1 where id < 0"):
			assert False,"Returned nonsense"
	except NoData:
		pass
	else:
		assert False,"Need to raise NoData"

	for i, in db.DoSelect("select id from test1 where id < 0", _empty=True):
		assert False,"Returned nonsense"
	print("Success.")

try: os.unlink("test.db")
except EnvironmentError: pass
run_test(1,"")
run_test(2,"auto_increment","create database if not exists test_sqlmix","drop table if exists test1")
run_test(3,"auto_increment","drop table if exists test1")

