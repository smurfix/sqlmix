======
sqlmix
======

`sqlmix` is a small Python module for accessing SQL databases.

It used to be included in my "collection of random stuff" GIT
archive at http://netz.smurf.noris.de/git/smurf.git/
(see there for ancient history).

Its new home is at git://github.com/smurfix/sqlmix.git or
http://netz.smurf.noris.de/git/sqlmix.git/

The "human" front-end is http://github.com/smurfix/sqlmix
or http://netz.smurf.noris.de/cgi/gitweb?p=sqlmix.git

---------
Rationale
---------

This code grew from my frustration with different Python database modules.

Specifically:

* The Python database interface specifies five different ways to
  safely embed arbitrary values in SQL statements.

* Databases have inconsistent names for their setup parameters.

* Multi-row SELECT statements are prime candidates for iteration.

Moreover,

* Executing a simple statement is verbose and requires multiple
  lines of what's essentially boilerplate code.

* Using a database from multiple threads is unsafe.

* How to use a database asynchronously, e.g. via Twisted "deferred" handlers,
  is non-obvious and, again, requires too much boilerplate code.

* Retrying an entire transaction (if it fails due to optimism) is difficult.
  There's no object representing the transaction.

`sqlmix` is intended as a tool for small one-off scripts where using an
object-oriented database wrapper like SqlAlchemywould be serious overkill.

There is support for asynchronous operation. Twisted and asyncio are
supported. Note that asyncio requires `DoSelect` to run within a
transaction.

-----
Usage
-----

Using this module is rather simple.

>>>	from sqlmix import Db,NoData
>>>	db = Db(database="testdb",username="testuser",password="testpass", isolation='repeatable read')
>>>	# db = Db(dbtype="sqlite",database="/tmp/testdb.sqlite")
>>>	db.Do("""\
...		create table test1 (
...			id integer primary key not null,
...			a varchar(255) not null default '',
...			b varchar(255) null
...		)
...	""")
>>>	db.commit()
>>>	A = db.Do("insert into test1(a,b) values (${a},${b})", a="one",b="OneOne")
>>>	B = db.Do("insert into test1(a) values (${a})", a="two")
>>>	C = db.Do("insert into test1(b) values (${b})", b="three")
>>>	db.commit()
>>>	a,b = db.DoFn("select a,b from test1 where id=${id}", id=A)
>>>	assert a=="one"; assert b=="OneOne"
>>>	for i, in db.DoSelect("select id from test1 order by id"):
>>>		n += 1
>>>		assert i==j
>>>		j += 1
>>>	assert n == 3
>>>	print "Success."

See "pydoc sqlmix" for further details.

Async use is trivially supported; the database commands return a Deferred /
an Awaitable. Async ``for`` loops (Python 3.5) are supported.

`DoFn` and `DoSelect` can return a dictionary instead of a list: pass
`_dict=True`. You may also pass a custom class, it will be instantiated for
every row.

Error Handling
--------------

There are two common error cases which _are_ handled:

* a `Do`, `DoFn` or `DoSelect` query does not return any results (or affect any rows).
  You can catch this with an `except NoData` handler, or ignore it with an
  `_empty=1` keyword (except for `DoFn`)

* a `DoFn` query returns more than one row. You can catch this with an
  `except ManyData` handler, or ignore it with a `limit 1` SQL clause.
  (If you do the latter, you should also add a unique `order by` clause.
  If you don't, you may get inconsistent results.)

Other error conditions are not handled. TODO. Specifically:

* connection timeout errors are not handled
  and no dummy queries to keep a connection alive are sent.

* "duplicate key" and "bad foreign key" errors need to be handled
  consistently.

Thread Safety
-------------

You may use a Db object in multiple threads. Each thread will get
its own low-level connection to the database. Therefore, any
transactions you open in a particular thread _must_ be closed
(i.e., committed or rolled back) from that thread.

Beware of database deadlocks. There is no (semi-)automatic retrying
mechanism. (TODO: There probably should be.)

---------
Copyright
---------

Gnu Public License, version 3.

I reserve the right to re-publish this library under a newer version of the
GPL, but (given the significant differences between v2 and v3) do not want
to allow an automagic re-licensing.

