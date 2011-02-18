# -*- coding: utf-8 -*-

"""Base class to collect database connections"""

from __future__ import generators
from smurf import Cf
from time import time,sleep
import string
import re

# shared
from sys import exc_info

from threading import local

def fixup_error(cmd):
	"""Append the full command to the error message"""
	e1,e2,e3 = exc_info()
	k=list(e2.args)
	k.append(cmd)
	e2.args=tuple(k)

class db_data(object):
	sequential = False
	def __init__(self, p="",**kwargs):
		"""host,port,database,username,password"""

		try: self.host = kwargs["host"]
		except KeyError:
			try: self.host = Cf["DATAHOST"+p]
			except KeyError: self.host = Cf["DATAHOST"]

		try: self.port = int(kwargs["port"])
		except KeyError:
			try: self.port = int(Cf["DATAPORT"+p])
			except KeyError:
				try: self.port = int(Cf["DATAPORT"])
				except KeyError: self.port = None

		try: self.dbname = kwargs["database"]
		except KeyError:
			try: self.dbname = Cf["DATABASE"+p]
			except KeyError: self.dbname = Cf["DATABASE"]

		try: self.username = kwargs["username"]
		except KeyError:
			try: self.username = Cf["DATAUSER"+p]
			except KeyError: self.username = Cf["DATAUSER"]

		try: self.password = kwargs["password"]
		except KeyError:
			try: self.password = Cf["DATAPASS"+p]
			except KeyError: self.password = Cf["DATAPASS"]


class _db_mysql(db_data):
	def __init__(self,p,**kwargs):
		self.DB = __import__("MySQLdb")
		self.DB.cursors = __import__("MySQLdb.cursors").cursors
		db_data.__init__(self,p,**kwargs)
#		if self.port:
#			self.host = self.host+":"+str(self.port)
#			self.port=None

	def conn(self,p=None,**kwargs):
		return self.DB.connect(db=self.dbname, host=self.host, user=self.username, passwd=self.password, port=self.port, charset="utf8")

class _db_odbc(db_data):
	def __init__(self,p,**kwargs):
		self.DB = __import__("mx.ODBC.iODBC")
		db_data.__init__(self,p,**kwargs)

	def conn(self,p=None,**kwargs):
		if self.port:
			self.host = self.host+":"+str(self.port)
			self.port=None

		return self.DB.connect(database=self.dbname, host=self.host, user=self.username, password=self.password)

class _db_postgres(db_data):
	def __init__(self,p,**kwargs):
		self.DB = __import__("pgdb")
		db_data.__init__(self,p,**kwargs)

	def conn(self,p=None,**kwargs):
		if self.port:
			self.host = self.host+":"+str(self.port)
			self.port=None

		return self.DB.connect(database=self.dbname,host=self.host, user=self.username, password=self.password)

class _db_sqlite(db_data):
	sequential = True
	def __init__(self,p,**kwargs):
		self.DB = __import__("pysqlite2.dbapi2")
		if hasattr(self.DB,"dbapi2"): self.DB=self.DB.dbapi2
		db_data.__init__(self,p,**kwargs)

	def conn(self,p=None,**kwargs):
		return self.DB.connect(self.dbname)



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
	"""Collects concurrent database connections"""

	def __init__(self, prefix=None, **kwargs):
		"""\
		Initialize connection collection.
		Keywords: host,port,database,username,password"""

		if prefix is None:
			prefix=""
		else:
			prefix="_"+prefix.upper()
		self.prefix=prefix
		try: dbtype = kwargs["dbtype"]
		except KeyError:
			try: dbtype=Cf["DATADB"+prefix]
			except KeyError: dbtype=Cf["DATADB"]
		self.DB = globals()["_db_"+dbtype](prefix, **kwargs)
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
		"""Allocate a database connection"""
		
		if not hasattr(self._c,"conn") or self._c.conn is None:
			if skip: return None

			Ex=None
			r = self.DB.conn(self.prefix)
			try: r.setconnectoption(self.DB.DB.SQL.AUTOCOMMIT, self.DB.DB.SQL.AUTOCOMMIT_OFF)
			except AttributeError:
				try: r.cursor(*self.CArgs).execute("SET AUTOCOMMIT=0")
				except Exception: pass # *sigh*
		
			try: r.cursor(*self.CArgs).execute("SET WAIT_TIMEOUT=7200") # 2h
			except Exception: pass

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
		
	def DoFn(self, _cmd, **keys):
		"""Database-specific DoFn function"""
		conn=self.conn()

		curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **keys)
		try:
			apply(curs.execute, _cmd)
		except:
			fixup_error(_cmd)
			raise
		val=curs.fetchone()

		if self._trace:
			self._trace("DoFn",_cmd,val)

		try:
			as_dict=keys["_dict"]
		except KeyError:
			as_dict=0
		else:
			if as_dict:
				def first(x): return x[0]
				as_dict = map(first, curs.description)

		if not val:
			raise NoData,_cmd
		if curs.fetchone():
			raise ManyData,_cmd

		if as_dict:
			val = dict(zip(as_dict,val))
		return val

	def Do(self, _cmd, **keys):
		"""Database-specific Do function"""
		conn=self.conn()
		curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **keys)
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
		if r == 0 and not keys.has_key("_empty"):
			raise NoData,_cmd
		return r

	def DoSelect(self, _cmd, **keys):
		"""Database-specific DoSelect function.
		
		'_store' is 0: force save on server
		'_store' is 1: force save on client

		'_head' is 1: first return is headers as text
		'_head' is 2: first return is DB header tuples

		'_dict' is 1: return entries as dictionary
		'_write' is 1: use the read/write server ## UNUSED
		'_empty' is 1: don't throw an error when no data are returned
		"""

		conn=self.conn()

		try:
			store=keys["_store"]
		except KeyError:
			store=None

		try:
			as_dict=keys["_dict"]
		except KeyError:
			as_dict=None

		if store:
			curs=conn.cursor(*self.CArgs)
		elif self.DB.dbtype == "mysql":
			curs=conn.cursor(self.DB.DB.cursors.SSCursor)
		else:
			curs=conn.cursor(*self.CArgs)

		_cmd = self.prep(_cmd, **keys)
		try:
			apply(curs.execute, _cmd)
		except:
			fixup_error(_cmd)
			raise
	
		def first(x): return x[0]
		try:
			head=keys["_head"]
		except KeyError:
			pass
		else:
			if head:
				if head>1:
					yield curs.description
				else:
					yield map(first, curs.description)

		try:
			as_dict=keys["_dict"]
		except KeyError:
			as_dict=0
		else:
			if as_dict:
				as_dict = map(first, curs.description)

		val=curs.fetchone()
		if not val:
			if self._trace:
				self._trace("DoSelect",_cmd,None)

			if not keys.has_key("_empty"):
				raise NoData,_cmd

		n=0
		while val != None:
			n += 1
			if as_dict:
				yield dict(zip(as_dict,val))
			else:
				yield val[:]

			val=curs.fetchone()


		if self._trace:
			self._trace("DoSelect",_cmd,n)

