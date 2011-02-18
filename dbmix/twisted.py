# -*- coding: utf-8 -*-

"""\
This class is a Twisted-compatible frontend to dbmix.Db.

It has the same interface, except that all Do* methods return a Deferred.

>>> dbi = dbmix.DbThread([args of dbmix.Db])
>>>
>>> @inlineCallbacks
>>> def foo(what_id):
>>>     with dbi() as db:
>>>         yield db.Do("delete from whatever where id=${id}", id=what_id, _empty=1)
>>>         res = yield db.DoSelect("select id from whatever", _empty=1)
>>>         for x in res:
>>>            print res
>>>         yield db.commit()
"""

from __future__ import generators,absolute_import
import dbmix
from time import time,sleep
import string
import re
import sys
from twisted.internet.defer import Deferred
from twisted.internet.threads import deferToThread
from threading import current_thread,Lock
from Queue import Queue

def debug(*a):
	return
	def pr(x):
		if isinstance(x,(basestring,int)):
			return unicode(x)
		else:
			return repr(x)
	s=" ".join((pr(x) for x in a))
	sys.stderr.write(s+"\n")

class DbThread(object):
	def __init__(self,*a,**k):
		self.db = []
		self.args = a
		self.kwargs = k
		self.kwargs['threaded'] = False
		self.lock = Lock()

	def _get_db(self):
		if self.db:
			return self.db.pop(0)
		return dbmix.Db(*self.args,**self.kwargs)
	def _put_db(self,db):
		self.db.append(db)
	def __call__(self):
		r = _DbThread(self)
		debug("NEW",id(r))
		return r

class _DbThread(object):
	def __init__(self,parent):
		self.parent = parent
		self.q = Queue()
		debug("INIT",id(self),id(self.q))
		self.done = deferToThread(self.run,self.q)

	def __enter__(self):
		return self
	def __exit__(self, a,b,c):
		if self.q is None:
			return False
		if a is None:
			self.commit()
		else:
			self.rollback()
		return False

	def run(self,q):
		db = self.parent._get_db()
		debug("START",id(q))
		res = None
		while True
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
			except Exception as e:
				if d:
					d.errback()
					d = None
			finally:
				if d: d.callback(res)
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

	def addDone(self,d):
		"""\
		Convenience method for terminating a transaction.
		
		>>> with dbi() as db:
		>>>     d = db.Do(...)
		>>>     db.addDone(d)

		"""
		d.addCallbacks(self.commit,self.rollback)
		
	def _do(self,job,*a,**k):
		"""Wrapper for calling the background thread."""
		d = Deferred()
		debug("QUEUE",id(self.q),job,a,k)
		self.q.put((d,job,a,k))
		return d
		
	def Do(self,*a,**k):
		return self._do("Do",*a,**k)
	def DoFn(self,*a,**k):
		return self._do("DoFn",*a,**k)
	def DoSelect(self,*a,**k):
		k["_store"] = 1
		return self._do("DoSelect",*a,**k)
	Do.__doc__ = dbmix.Db.Do.__doc__ + "\nReturns a Deferred.\n"
	DoFn.__doc__ = dbmix.Db.DoFn.__doc__ + "\nReturns a Deferred.\n"
	DoSelect.__doc__ = dbmix.Db.DoSelect.__doc__ + "Returns a Deferred.\n"

