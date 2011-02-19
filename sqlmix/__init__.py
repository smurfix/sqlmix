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
import os
import re
from sys import exc_info
from threading import local
import ConfigParser as configparser

def fixup_error(cmd):
	"""Append the full command to the error message"""
	e1,e2,e3 = exc_info()
	k=list(e2.args)
	k.append(cmd)
	e2.args=tuple(k)

class db_data(object):
	sequential = False
	def __init__(self, cfg,pre, **kwargs):
		"""standard databases: host,port,database,username,password"""
		for f in "host port database username password".split():
			try:
				try:
					v = kwargs[f]
				except KeyError:
					v = cfg.get(pre,f)
			except configparser.NoOptionError:
				pass
			else:
				setattr(self,f,v)

class _db_mysql(db_data):
	def __init__(self,cfg,pre,**kwargs):
		self.DB = __import__("MySQLdb")
		self.DB.cursors = __import__("MySQLdb.cursors").cursors
		db_data.__init__(self,cfg,pre,**kwargs)

	def conn(self,**kwargs):
		return self.DB.connect(db=self.database, host=self.host, user=self.username, passwd=self.password, port=self.port, charset="utf8")

class _db_odbc(db_data):
	def __init__(self,cfg,pre,**kwargs):
		self.DB = __import__("mx.ODBC.iODBC")
		db_data.__init__(self,cfg,pre,**kwargs)

	def conn(self,**kwargs):
		if self.port:
			self.host = self.host+":"+str(self.port)
			self.port=None

		return self.DB.connect(database=self.database, host=self.host, user=self.username, password=self.password)

class _db_postgres(db_data):
	def __init__(self,cfg,pre,**kwargs):
		self.DB = __import__("pgdb")
		db_data.__init__(self,cfg,pre,**kwargs)

	def conn(self,**kwargs):
		if self.port:
			self.host = self.host+":"+str(self.port)
			self.port=None

		return self.DB.connect(database=self.database,host=self.host, user=self.username, password=self.password)

class _db_sqlite(db_data):
	sequential = True
	def __init__(self,cfg,pre,**kwargs):
		self.DB = __import__("pysqlite2.dbapi2")
		if hasattr(self.DB,"dbapi2"): self.DB=self.DB.dbapi2
		db_data.__init__(self,cfg,pre,**kwargs)

	def conn(self,**kwargs):
		return self.DB.connect(self.database)

_databases = {
	    "mysql": _db_mysql,
	    "odbc": _db_odbc,
	    "postgres": _db_postgres,
	    "sqlite": _db_sqlite,
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

class Db(object):
	"""\
	Main database connection object.
	"""

	# These variables cache whether the database supports turning off
	# autocommit, or setting the read wait timeout.
	_set_ac1 = True
	_set_ac2 = True
	_set_timeout = True
	def __init__(self, name=None, cfgfile=None, **kwargs):
		"""\
		Initialize me. Read default parameters from section *name
		in INI-style configuration file(s) CFGFILE.
		(Default: section 'database' in ./sqlmix.cfg or $HOME/sqlmix.cfg)

		Keywords: dbtype,host,port,database,username,password
		"""

		if cfgfile is None:
			cfgfile=["./sqlmix.cfg", os.path.expanduser("~/.sqlmix.cfg")]
		if name is None:
			name="database"
		self.cfg=configparser.ConfigParser({'dbtype':'mysql', 'host':'localhost'})
		self.cfg.read(cfgfile)
		if not self.cfg.has_section(name):
			self.cfg.add_section(name)

		try:
			dbtype = kwargs["dbtype"]
		except KeyError:
			dbtype=self.cfg.get(name,"type")
		self.DB = _databases[dbtype](self.cfg,name, **kwargs)
		self.DB.dbtype=dbtype

		self._c = local()
		self._c.conn=None
		self._trace=None

#		if dbtype == "mysql":
#			self.CArgs = (self.DB.DB.cursors.CursorNW,)
#		else:
		self.CArgs = ()
#

		(self.arg_init, self.arg_do, self.arg_done) \
			 = _parsers[self.DB.DB.paramstyle]
		
	def conn(self, skip=False):
		"""Create a connection to the underlying database"""
		
		if not hasattr(self._c,"conn") or self._c.conn is None:
			if skip: return None

			Ex=None
			r = self.DB.conn()

			if self._set_ac1:
				try: r.setconnectoption(self.DB.DB.SQL.AUTOCOMMIT, self.DB.DB.SQL.AUTOCOMMIT_OFF)
				except AttributeError: self._set_ac1 = False
				else: self._set_ac2 = False
			if self._set_ac2:
				try: r.cursor(*self.CArgs).execute("SET AUTOCOMMIT=0")
				except Exception: self._set_ac2 = False
		
			if self._set_timeout:
				try: r.cursor(*self.CArgs).execute("SET WAIT_TIMEOUT=7200") # 2h
				except Exception: self._set_timeout = False

#			try:
#				r.stringformat = self.DB.DB.UNICODE_STRINGFORMAT
#				r.encoding = 'utf-8'
#			except AttributeError:
#				pass

			r.commit()
			self._c.conn = r
		#r.cursor(*self.CArgs).execute('BEGIN')
		return self._c.conn

	def commit(self):
		if self._trace:
			self._trace("Commit","","")
		c = self.conn(skip=True)
		if c: c.commit()

		# callbacks
		if hasattr(self._c,"committed") and self._c.committed:
			x = self._c.committed
			self._c.committed = None
			for proc,a,k in x[::-1]:
				proc(*a,**k)
		self._c.rolledback = None

		#self._conn.cursor(*self.CArgs).execute('BEGIN')

	def rollback(self):
		if self._trace:
			self._trace("RollBack","","")
		c = self.conn(skip=True)
		if c: c.rollback()

		# cancel callbacks
		if hasattr(self._c,"rolledback") and self._c.rolledback:
			x = self._c.rolledback
			self._c.rolledback = None
			for proc,a,k in x[::-1]:
				proc(*a,**k)
		self._c.committed = None

		#self._conn.cursor(*self.CArgs).execute('BEGIN')

	def call_committed(self,proc,*a,**k):
		if not hasattr(self._c,"committed") or self._c.committed is None:
			self._c.committed = []
		self._c.committed.append((proc,a,k))

	def call_rolledback(self,proc,*a,**k):
		if not hasattr(self._c,"rolledback") or self._c.rolledback is None:
			self._c.rolledback = []
		self._c.rolledback.append((proc,a,k))

	def prep(self,_cmd,**kwargs):

		args = self.arg_init()
		def _prep(name):
			return self.arg_do(name.group(1), args, kwargs)
		
		_cmd = re.sub(r"\$\{([a-zA-Z][a-zA-Z_0-9]*)\}",_prep,_cmd)
		return self.arg_done(_cmd,args)
		
	def DoFn(self, _cmd, **kv):
		"""Database-specific DoFn function"""
		conn=self.conn()
		curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **kv)
		try:
			apply(curs.execute, _cmd)
		except:
			fixup_error(_cmd)
			raise
		val=curs.fetchone()

		if self._trace:
			self._trace("DoFn",_cmd,val)

		as_dict=kv.get("_dict",None)
		if as_dict:
			as_dict = map(lambda x:x[0], curs.description)

		if not val:
			raise NoData,_cmd
		if curs.fetchone():
			raise ManyData,_cmd

		if as_dict:
			val = dict(zip(as_dict,val))
		return val

	def Do(self, _cmd, **kv):
		"""Database-specific Do function"""
		conn=self.conn()
		curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **kv)
		try:
			apply(curs.execute, _cmd)
		except:
			fixup_error(_cmd)
			raise
		r = curs.lastrowid
		if not r:
			r = curs.rowcount

		if self._trace:
			self._trace("DoFn",_cmd,r)
		if r == 0 and not kv.has_key("_empty"):
			raise NoData,_cmd
		return r

	def DoSelect(self, _cmd, **kv):
		"""Database-specific DoSelect function.
		
		'_store' is 0: force save on server
		'_store' is 1: force save on client

		'_head' is 1: first return is headers as text
		'_head' is 2: first return is DB header tuples

		'_dict' is 1: return entries as dictionary
		'_write' is 1: use the read/write server ## UNUSED
		'_empty' is 1: don't throw an error when no data are returned
		'_callback': pass values to this proc, return count
		             otherwise return iterator for values
		"""
		cb = kv.get('_callback',None)
		if cb:
			n = 0
			for x in self._DoSelect(_cmd, **kv):
				cb(x)
				n += 1
			return n
		else:
			return self._DoSelect(_cmd, **kv)

	def _DoSelect(self, _cmd, **kv):
		conn=self.conn()

		store=kv.get("_store",None)
		as_dict=kv.get("_dict",None)

		if store:
			curs=conn.cursor(*self.CArgs)
		elif self.DB.dbtype == "mysql":
			curs=conn.cursor(self.DB.DB.cursors.SSCursor)
		else:
			curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **kv)
		try:
			apply(curs.execute, _cmd)
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
			as_dict = map(lambda x:x[0], curs.description)

		val=curs.fetchone()
		if not val:
			if self._trace:
				self._trace("DoSelect",_cmd,None)

			if not kv.has_key("_empty"):
				raise NoData,_cmd

		n=0
		while val != None:
			n += 1
			if as_dict:
				yield dict(zip(as_dict,val))
			else:
				yield val[:]
				# need to copy because the array may be re-used
				# internally by the database driver, but the consumer
				# might want to store/modify it
			val=curs.fetchone()

		if self._trace:
			self._trace("DoSelect",_cmd,n)

