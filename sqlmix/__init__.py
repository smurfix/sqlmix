# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import

"""\
This module provides a thread-safe way to access any Python SQL database with
* A common and clear syntax for placing values in SQL statements.
   >> foo,bar = db.DoFn("select ... where id=${key}", key=whatever)
* single-line SQL commands
* Common parameters for database setup (no more "db" vs. "database")
* Single-line SQL statements and single-row SELECTs:
   >> db.Do("insert ...")
   >> foo,bar = db.DoFn("select ... where id=123")
* SELECT statements as iterators:
   >> for a,b in db.DoSelect("select ..."):
   >>     ...
"""
#
#    Copyright (C) 2011 Matthias urlichs <smurf@smurf.noris.de>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

from time import time,sleep
import sys
import os
import re
from sys import exc_info
from threading import local

class CommitThread(Exception):
	u"""\
		If you leave a database handler's with … block by raising an
		exception descending from this class, the transaction will be
		committed instead of being rolled back.
		"""
	pass

class FakeLocal(object):
	"""\
		This connection is only used by one thread,
		so don't bother with thread-local variables.
		"""
	pass

__all__ = ["Db","NoData","ManyData"]

def fixup_error(cmd):
	"""Append the full command to the error message"""
	e1,e2,e3 = exc_info()
	k=list(e2.args)
	k.append(cmd)
	e2.args=tuple(k)

class _NOTGIVEN: pass

class db_data(object):
	sequential = False
	_store = 1 # safe default
	_cursor = True
	def __init__(self, **kwargs):
		"""standard keywords: host,port,database,username,password"""
		for f in "host port database username password".split():
			v = kwargs.pop(f,_NOTGIVEN)
			if v is _NOTGIVEN:
				continue
			if f == "port":
				v=int(v)
			setattr(self,f,v)
		kwargs.setdefault("charset","utf8")
		self.kwargs = kwargs

class _db_mysql(db_data):
	_store = 1
	host="localhost"
	port=3306
	def __init__(self, **kwargs):
		self.DB = __import__("MySQLdb")
		self.DB.cursors = __import__("MySQLdb.cursors").cursors
		super(_db_mysql,self).__init__(**kwargs)

	def conn(self):
		return self.DB.connect(db=self.database, host=self.host, user=self.username, passwd=self.password, port=self.port, **self.kwargs)

class _db_ultramysql(db_data):
	_store = 1
	_cursor = False
	host="localhost"
	port=3306
	def __init__(self, **kwargs):
		self.DB = __import__("umysql")
		super(_db_ultramysql,self).__init__(**kwargs)
		self.DB.paramstyle = 'format'

	def conn(self):
		c = self.DB.Connection()
		c.connect(self.host, self.port, self.username, self.password, self.database, False, "utf8")
		return c

class _db_odbc(db_data):
	def __init__(self, **kwargs):
		self.DB = __import__("mx.ODBC.iODBC")
		super(_db_odbc,self).__init__(**kwargs)

	def conn(self):
		if self.port:
			self.host = self.host+":"+str(self.port)
			self.port=None

		return self.DB.connect(database=self.database, host=self.host, user=self.username, password=self.password)

class _db_postgres(db_data):
	def __init__(self, **kwargs):
		self.DB = __import__("psycopg2")
		super(_db_postgres,self).__init__(**kwargs)

	def conn(self):
		return self.DB.connect(database=self.database,host=self.host, user=self.username, password=self.password, port=self.port)

class _db_sqlite(db_data):
	sequential = True
	def __init__(self, **kwargs):
		self.DB = __import__("sqlite3.dbapi2")
		if hasattr(self.DB,"dbapi2"): self.DB=self.DB.dbapi2
		super(_db_sqlite,self).__init__(**kwargs)

	def conn(self):
		return self.DB.connect(self.database)

_databases = {
	    "mysql": _db_mysql,
	    "odbc": _db_odbc,
	    "postgres": _db_postgres,
	    "sqlite": _db_sqlite,
	    "ultramysql": _db_ultramysql,
}


## possible parsers

## qmark: '?'
def _init_qmark():
	return []
def _do_qmark(name, arg, params):
	arg.append(params[name])
	return "?"
def _done_qmark(cmd,args):
	return (cmd,tuple(args))

## numeric: ':n'
def _init_numeric():
	return []
def _do_numeric(name, arg, params):
	arg.append(params[name])
	return ":" + str(len(arg))
def _done_numeric(cmd,args):
	return (cmd,args)

## named: ':name'

def _init_named():
	return {}
def _do_named(name, arg, params):
	arg[name] = params[name]
	return ":" + name
def _done_named(cmd,args):
	return (cmd,args)

## format: '%s'

def _init_format():
	return []
def _do_format(name, arg, params):
	arg.append(params[name])
	return "%s"
def _done_format(cmd,args):
	return (cmd,args)

## pyformat: '%(name)s'

def _init_pyformat():
	return {}
def _do_pyformat(name, arg, params):
	arg[name] = params[name]
	return "%(" + name + ")s"
def _done_pyformat(cmd,args):
	return (cmd,args)

_parsers = {
		"qmark"   : (_init_qmark,_do_qmark,_done_qmark),
		"numeric" : (_init_numeric,_do_numeric,_done_numeric),
		"named"   : (_init_named,_do_named,_done_named),
		"format"  : (_init_format,_do_format,_done_format),
		"pyformat": (_init_pyformat,_do_pyformat,_done_pyformat),
 	   }


## these might be based on StopIteration instead.  Too dangerous ???

class NoData(Exception):
	pass
class ManyData(Exception):
	pass
#class NoDatabase(Exception):
#	pass

class DbPrep(object):
	"""Base class for command prep"""
	def __init__(self):
		if hasattr(self.DB,'paramstyle'):
			paramstyle = self.DB.paramstyle
		else:
			paramstyle = self.DB.DB.paramstyle
		(self.arg_init, self.arg_do, self.arg_done) \
			 = _parsers[paramstyle]
	def prep(self,_cmd,**kwargs):

		args = self.arg_init()
		def _prep(name):
			return self.arg_do(name.group(1), args, kwargs)
		
		_cmd = re.sub(r"\$\{([a-zA-Z][a-zA-Z_0-9]*)\}",_prep,_cmd)
		return self.arg_done(_cmd,args)
		

class Db(DbPrep):
	"""\
	Main database connection object.

	Internally, manages one back-end connection per thread.

	Possible keywords: dbtype,host,port,database,username,password; config=inifile,cfg=section
	"""

	# These variables cache whether the database supports turning off
	# autocommit, or setting the read wait timeout.
	_set_ac1 = True
	_set_ac2 = True
	_set_timeout = True
	_set_isolation = True

	def __init__(self, cfg=None, **kwargs):
		if cfg is not None:
			try:
				cffile = kwargs.pop('config')
			except KeyError:
				from os.path import expanduser as home
				cffile = home("~/.sqlmix.conf")
			if isinstance(cffile,str):
				from configparser import ConfigParser
				cfp = ConfigParser()
				cfp.read(cffile)
			else:
				cfp = cffile
			args = dict(cfp.items(cfg))
			args.update(kwargs)
			kwargs = args

		self._trace = kwargs.pop("trace",None)

		dbtype = kwargs.pop("dbtype","mysql")
		self.DB = _databases[dbtype](**kwargs)
		self.DB.dbtype=dbtype
		if self._trace is not None:
			self._trace("INIT",dbtype,kwargs)

		if kwargs.pop("_single_thread",False):
			self._c = FakeLocal()
		else:
			self._c = local()
		self._c.conn=None

#		if dbtype == "mysql":
#			self.CArgs = (self.DB.DB.cursors.CursorNW,)
#		else:
		self.CArgs = ()
		self.isolation = kwargs.pop("isolation",None)
#

		super(Db,self).__init__()
		
	def _conn(self, skip=False):
		"""Create a connection to the underlying database."""
		
		if not hasattr(self._c,"conn") or self._c.conn is None:
			if skip: return None

			Ex=None
			r = self.DB.conn()

			if self._set_ac1:
				try: r.setconnectoption(self.DB.DB.SQL.AUTOCOMMIT, self.DB.DB.SQL.AUTOCOMMIT_OFF)
				except AttributeError: self._set_ac1 = False
				else: self._set_ac2 = False
			if self._set_ac2:
				try:
					if self.DB._cursor:
						r.cursor(*self.CArgs).execute("SET AUTOCOMMIT=0")
					else:
						r.query("SET AUTOCOMMIT=0")
				except Exception: self._set_ac2 = False
		
			if self._set_timeout:
				try: 
					if self.DB._cursor:
						r.cursor(*self.CArgs).execute("SET WAIT_TIMEOUT=7200") # 2h
					else:
						r.query("SET WAIT_TIMEOUT=7200")
				except Exception: self._set_timeout = False

			if self._set_isolation and self.isolation:
				try:
					if self.DB._cursor:
						r.cursor(*self.CArgs).execute("SET SESSION TRANSACTION ISOLATION LEVEL "+self.isolation)
					else:
						r.query("SET SESSION TRANSACTION ISOLATION LEVEL "+self.isolation)
						
				except Exception: self._set_isolation = False

#			try:
#				r.stringformat = self.DB.DB.UNICODE_STRINGFORMAT
#				r.encoding = 'utf-8'
#			except AttributeError:
#				pass

			self._commit(r)
			self._c.conn = r
		#r.cursor(*self.CArgs).execute('BEGIN')
		return self._c.conn

	def _commit(self,c,cmd="commit"):
		if(hasattr(c,cmd)):
			getattr(c,cmd)()
		elif self.DB._cursor:
			c.cursor(*self.CArgs).execute(cmd)
		else:
			c.query(cmd)

	def close(self):
		"""\
		Close the current connection, if any.
		"""
		self.rollback()

		c = self._conn(skip=True)
		if c:
			if self._trace is not None:
				self._trace("Close","","")
			c.close()
		else:
			if self._trace is not None:
				self._trace("Close","NoConn","")

	def commit(self):
		"""\
		Commit the current transaction.
		
		This calls all procedures that have been registered with `call_committed`
		in reverse order, and discards all calls registered with `call_rolledback`.
		"""
		if self._trace is not None:
			self._trace("Commit","","")
		c = self._conn(skip=True)
		if c:
			self._commit(c)

		# callbacks
		self._c.rolledback = None
		if hasattr(self._c,"committed") and self._c.committed:
			x = self._c.committed
			self._c.committed = None
			for proc,a,k in x[::-1]:
				proc(*a,**k)

		#self._conn.cursor(*self.CArgs).execute('BEGIN')

	def rollback(self):
		"""\
		Roll back the current transaction.
		
		This calls all procedures that have been registered with `call_rolledback`
		in reverse order, and discards all calls registered with `call_committed`.
		"""
		if self._trace is not None:
			self._trace("RollBack","","")
		c = self._conn(skip=True)
		if c:
			self._commit(c,"rollback")

		# cancel callbacks
		self._c.committed = None
		if hasattr(self._c,"rolledback") and self._c.rolledback:
			x = self._c.rolledback
			self._c.rolledback = None
			for proc,a,k in x[::-1]:
				proc(*a,**k)

		#self._conn.cursor(*self.CArgs).execute('BEGIN')

	def call_committed(self,proc,*a,**k):
		"""\
		Remember to call a procedure (with given parameters) after committing the
		current transaction.
		"""
		if not hasattr(self._c,"committed") or self._c.committed is None:
			self._c.committed = []
		self._c.committed.append((proc,a,k))

	def call_rolledback(self,proc,*a,**k):
		"""\
		Remember to call a procedure (with given parameters) after rolling back the
		current transaction.
		"""
		if not hasattr(self._c,"rolledback") or self._c.rolledback is None:
			self._c.rolledback = []
		self._c.rolledback.append((proc,a,k))

	def DoFn(self, _cmd, **kv):
		"""Select one (and only one) row.

		Example:
		>>>	i,before = db.DoFn("select id,last_login from sometable where name=${myname}", myname="Fred")

		This function always returns an array of values.
		Thus, single-value assignments need a comma after the variable name.
		>>>	i, = db.DoFn("select id from sometable where name=${myname}", myname="Fred")
		
		Special keywords:

		_dict is True: return a column/value dictionary instead of an array.
		_dict is a type: as before, but use that. 
		>>>	info = db.DoFn("select * from sometable where name=${myname}",myname="Fred", _dict=True)

		"""
		conn=self._conn()
		_cmd = self.prep(_cmd, **kv)
		try:
			if self.DB._cursor:
				curs=conn.cursor(*self.CArgs)
				curs.execute(*_cmd)
			else:
				curs=conn.query(*_cmd)
		except:
			fixup_error(_cmd)
			raise

		if hasattr(curs,'fetchone'):
			val = curs.fetchone()
		elif not curs.rows:
			val = None
		else:
			val = curs.rows.pop(0)

		if self._trace is not None:
			self._trace("DoFn",_cmd,val)
		if not val:
			raise NoData(_cmd)

		as_dict=kv.get("_dict",None)
		if as_dict:
			if as_dict is True:
				as_dict = dict
			names = map(lambda x:x[0], curs.description)

		if curs.fetchone() if hasattr(curs,'fetchone') else curs.rows:
			raise ManyData(_cmd)

		if as_dict:
			val = as_dict(zip(names,val))
		return val

	def Do(self, _cmd, **kv):
		"""Database-specific Do function"""
		conn=self._conn()
		_cmd = self.prep(_cmd, **kv)

		try:
			if self.DB._cursor:
				curs=conn.cursor(*self.CArgs)
				curs.execute(*_cmd)
			else:
				curs=conn.query(*_cmd)
		except:
			fixup_error(_cmd)
			raise

		if isinstance(curs,(tuple,list)): # ultramysql
			r = curs[1]
			if not r:
				r = curs[0]
		else:
			r = curs.lastrowid
			if not r:
				r = curs.rowcount

		if self._trace is not None:
			self._trace("DoFn",_cmd,r)
		if r == 0 and not '_empty' in kv:
			raise NoData(_cmd)
		return r

	def DoSelect(self, _cmd, **kv):
		"""Select one or more rows from a database.

		>>>	for i,name in db.DoSelect("select id,name from sometable where name like ${pattern}", pattern='f%d'):
		...		print i,name
		
		The iterator always returns an array of values.
		Thus, single-value assignments need a comma after the variable name.
		>>>	for name, in db.DoSelect("select name from sometable where name like ${pattern}", pattern='f%d'):
		...		print name
		
		Special keywords:

		'_store' is 0: force saving the result on the server
		'_store' is 1: force saving the result on the client
		Otherwise:     save on the server if the backend supports multiple
		               concurrent cursors on a single connection

		'_head' is 1: first return is headers as text
		'_head' is 2: first return is DB header tuples

		'_dict' is True: yield entries as dictionary instead of list
		'_dict' is a type: as before, but use that. 
		'_empty' is True: don't throw an error when no data are returned
		'_callback': pass rows to a procedure (either as arguments or as
		             keywords, depending on _dict), return row count

		"""
		cb = kv.get('_callback',None)
		if cb:
			n = 0
			for x in self._DoSelect(_cmd, **kv):
				if kv.get('_dict',None):
					cb(**x)
				else:
					cb(*x)
				n += 1
			return n
		else:
			return self._DoSelect(_cmd, **kv)

	def _DoSelect(self, _cmd, **kv):
		conn=self._conn()

		store=kv.get("_store",self.DB._store)

		if self.DB._cursor:
			if store:
				curs=conn.cursor(*self.CArgs)
			elif self.DB.dbtype == "mysql":
				curs=conn.cursor(self.DB.DB.cursors.SSCursor)
			else:
				curs=conn.cursor(*self.CArgs)
		_cmd = self.prep(_cmd, **kv)
		try:
			if self.DB._cursor:
				curs.execute(*_cmd)
			else:
				curs = conn.query(*_cmd)
		except:
			fixup_error(_cmd)
			raise

	
		head = kv.get("_head",None)
		if head:
			if head>1:
				yield curs.description
			else:
				yield map(lambda x:x[0], curs.description)

		as_dict=kv.get("_dict",None)
		if as_dict:
			if as_dict is True:
				as_dict = dict
			names = map(lambda x:x[0], curs.description)

		val = curs.fetchone() if hasattr(curs,'fetchone') else curs.rows.pop(0) if curs.rows else None
		if not val:
			if self._trace is not None:
				self._trace("DoSelect",_cmd,None)

			if '_empty' not in kv:
				raise NoData(_cmd)

		n=0
		while val is not None:
			n += 1
			if as_dict:
				yield as_dict(zip(names,val))
			else:
				yield val[:]
				# need to copy because the array may be re-used
				# internally by the database driver, but the consumer
				# might want to store/modify it
			val = curs.fetchone() if hasattr(curs,'fetchone') else curs.rows.pop(0) if curs.rows else None

		if self._trace is not None:
			self._trace("DoSelect",_cmd,n)


	def __call__(self):
		return self

	def __enter__(self):
		return self

	def __exit__(self, a,b,c):
		if b is None or isinstance(b,CommitThread):
			self.commit()
		else:
			self.rollback()
		return False

