#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import,print_function

"""\
Simple test script
"""

import os
from sqlmix.async_ import Db,ManyData,NoData
from warnings import filterwarnings

filterwarnings("ignore",category=RuntimeWarning,lineno=15)

async def run_test(x,ai):
 async with Db("db"+str(x),config="test.ini") as dbp:

  try:
        await dbp.Do("drop table if exists test1")
  except NoData:
        pass
  await dbp.Do("""\
        create table test1 (
            id integer primary key not null %s,
            a varchar(255) not null default '',
            b varchar(255) null
        )
    """ % (ai,), _empty=True)

  async with dbp() as db:
    A = await db.Do("insert into test1(a,b) values (${a},${b})", a="one",b="OneOne")
    B = await db.Do("insert into test1(a) values (${a})", a="two")
    C = await db.Do("insert into test1(b) values (${b})", b="three")
  async with dbp() as db:
    async def chk(db):
        a,b = await db.DoFn("select a,b from test1 where id=${id}", id=A)
        assert a=="one"; assert b=="OneOne"
        a,b = await db.DoFn("select a,b from test1 where id=${id}", id=B)
        assert a=="two"; assert b is None
        a,b = await db.DoFn("select a,b from test1 where id=${id}", id=C)
        assert a==""; assert b=="three"
    await chk(db)
    await db.Do("delete from test1 where id=${id}", id=B)
    await db.rollback()

  async with dbp() as db:
    n=0; j=A
    async for i, in db.DoSelect("select id from test1 order by id"):
        n += 1
        assert i==j, (i,j)
        j += 1
    assert n == 3
    try:
        await db.DoFn("select id from test1")
    except ManyData:
        pass
    else:
        assert False,"Need to raise ManyData"

    try:
        async for i, in db.DoSelect("select id from test1 where id < 0"):
            assert False,"Returned nonsense"
    except NoData:
        pass
    else:
        assert False,"Need to raise NoData"

    async for i, in db.DoSelect("select id from test1 where id < 0", _empty=True):
        assert False,"Returned nonsense"
  print("Success.")

async def run_tests():
    await run_test(2,"auto_increment")
#run_test(3,"auto_increment","drop table if exists test1")

import anyio
anyio.run(run_tests, backend="trio")
