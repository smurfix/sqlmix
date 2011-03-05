# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import

"""\
This class is a Twisted-compatible frontend to sqlmix.Db.

It has the same interface, except that all Do* methods return a Deferred.
Internally, it works by delegating all SQL commands to a separate thread.

 >> dbi = sqlmix.DbPool([args of sqlmix.Db])
 >>
 >> @inlineCallbacks
 >> def foo(what_id):
 >>     with dbi() as db:
 >>         yield db.Do("delete from whatever where id=${id}", id=what_id, _empty=1)
 >>         res = yield db.DoSelect("select id from whatever", _empty=1)
 >>         for x in res:
 >>            print res
 >>         yield db.commit()
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


import sqlmix
from time import time,sleep
import string
import re
import sys
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure
from twisted.internet.threads import deferToThread
from threading import Lock
from Queue import Queue
__all__ = ["DbPool"]

def _print_error(f):
	f.printTraceback(file=sys.stderr)

def debug(*a):
	return
	def pr(x):
		if isinstance(x,(tuple,list)):
			return "\n".join((pr(y).strip("\n") for y in x))
		elif isinstance(x,(basestring,int)):
			return unicode(x)
		else:
			return repr(x)
	s=" ".join((pr(x) for x in a))
	sys.stderr.write(s+"\n")

class CommitThread(Exception):
	u"""\
		If you leave a database handler's with … block by raising an
		exception descending from this class, the transaction will be
		committed instead of being rolled back.
		"""
	pass

class DbPool(object):
	"""\
	Manage a pool of database connections.

	TODO: shrink the pool.
	TODO: issue periodic keepalive requests.
	"""
	def __init__(self,*a,**k):
		"""\
		Create a pool of database connections, for processing (a sequence of)
		SQL commands in the background.
		"""
		self.db = []
		self.args = a
		self.kwargs = k
		self.lock = Lock()

	def _get_db(self):
		if self.db:
			return self.db.pop(0)
		return sqlmix.Db(*self.args,**self.kwargs)
	def _put_db(self,db):
		self.db.append(db)
	def __call__(self):
		"""\
		Get a new connection from the database pool (or start a new one)
		and return a thread handler.

		Usage:
		>>> @inlineCallbacks
		>>> def something(...):
		>>> 	dbpool = DbPool(...) # arguments like sqlmix.Db()
		>>>		with dbpool() as db:
		>>>			d = db.Do("...")
		>>>			assert(isinstance(d,twisted.internet.defer.Deferred))
		>>>			res = yield d

		The transaction will be committed if you leave the "with" block
		normally or with a CommitThread exception. Otherwise, it will be
		rolled back.

		Note that you must use the @inlineCallbacks method if you want
		to use the database conection more than once. Otherwise, control
		will have left the "with" block and the connection will be dead.
		"""
		r = _DbThread(self)
		debug("NEW",id(r))
		return r

class _DbThread(object):
	def __init__(self,parent):
		self.parent = parent
		self.q = Queue()
		debug("INIT",id(self),id(self.q))
		self.done = deferToThread(self.run,self.q)

		self.committed = []
		self.rolledback = []
		self.done.addCallback(self._run_committed)
		self.done.addErrback(self._run_rolledback)
		self.done.addErrback(_print_error)

	def __enter__(self):
		return self
	def __exit__(self, a,b,c):
		if self.q is None:
			return False
		if b is None or isinstance(b,CommitThread):
			self.commit()
		else:
			from traceback import format_exception
			debug("EXIT ON ERROR",format_exception(a,b,c))
			self.rollback()
		return False

	def _run_committed(self,r):
		try:
			for proc,a,k in self.committed[::-1]:
				debug("AFTER COMMIT",proc,a,k)
				proc(*a,**k)
		finally:
			self.committed = ()
		self.rolledback = ()
		return r
	def _run_rolledback(self,r):
		self.committed = ()
		try:
			for proc,a,k in self.rolledback[::-1]:
				debug("AFTER ROLLBACK",proc,a,k)
				proc(*a,**k)
		finally:
			self.rolledback = ()
		return r

	def run(self,q):
		db = self.parent._get_db()
		debug("START",id(q))
		res = None
		while True:
			d = None
			try:
				d,proc,a,k = q.get()
				res = None
				debug("DO",id(q),proc,a,k)
				if proc == "COMMIT":
					debug("COMMIT",a,k)
					res = k.get('res',None)
					db.commit()
					break
				elif proc == "ROLLBACK":
					debug("ROLLBACK",a,k)
					res = k.get('res',None)
					db.rollback()
					break
				res = getattr(db,proc)(*a,**k)
			except BaseException as e:
				if d:
					debug("EB",id(q),d,res)
					reactor.callFromThread(d.errback,Failure())
					d = None
				else:
					raise
			finally:
				if d:
					debug("CB",id(q),d,res)
					reactor.callFromThread(d.callback,res)
				debug("DID",id(q),res)
		self.parent._put_db(db)
		debug("STOP",id(q))
		return res

	def __del__(self):
		if self.q is None:
			return
		self.q.put((None,"COMMIT",[],{}))
		debug("DEAD",id(self),id(self.q),a)
		self.q = None

	def commit(self,res=None):
		debug("CALL COMMIT",id(self),id(self.q),res)
		self.q.put((None,"COMMIT",[],{'res':res}))
		self.q = None
		return self.done
	def rollback(self,res=None):
		debug("CALL ROLLBACK",id(self),id(self.q),res)
		self.q.put((None,"ROLLBACK",[],{'res':res}))
		self.q = None
		return self.done

	def call_committed(self,proc,*a,**k):
		self.committed.append((proc,a,k))
	def call_rolledback(self,proc,*a,**k):
		self.rolledback.append((proc,a,k))

	def _do(self,job,*a,**k):
		"""Wrapper for calling the background thread."""
		d = Deferred()
		debug("QUEUE",id(self.q),job,a,k)
		self.q.put((d,job,a,k))
		def _log(r):
			debug("BACK",id(self.q),r)
			return r
		d.addBoth(_log)
		return d
		
	def Do(self,*a,**k):
		return self._do("Do",*a,**k)
	def DoFn(self,*a,**k):
		return self._do("DoFn",*a,**k)
	def DoSelect(self,*a,**k):
		k["_store"] = 1
		return self._do("DoSelect",*a,**k)
	Do.__doc__ = sqlmix.Db.Do.__doc__ + "\nReturns a Deferred.\n"
	DoFn.__doc__ = sqlmix.Db.DoFn.__doc__ + "\nReturns a Deferred.\n"
	DoSelect.__doc__ = sqlmix.Db.DoSelect.__doc__ + "Returns a Deferred.\n"

