# -*- coding: utf-8 -*-

""" class to do database stuff via Twisted """

from __future__ import generators,absolute_import
from smurf import Db
from time import time,sleep
import string
import re
import sys
from twisted.internet.defer import Deferred
from twisted.internet.threads import deferToThread
from threading import current_thread,Lock
from Queue import Queue

# shared
from sys import exc_info

NoData=Db.NoData
ManyData=Db.ManyData

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
		return Db.Db(*self.args,**self.kwargs)
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
		running = True
		debug("START",id(q))
		res = None
		while running:
			d = None
			try:
				d,proc,a,k = q.get()
				res = None
				debug("DO",id(q),proc,a,k)
				if proc == "COMMIT":
					debug("COMMIT",a,k)
					res = k.get('res',None)
					db.commit()
					running = False
				elif proc == "ROLLBACK":
					debug("ROLLBACK",a,k)
					res = k.get('res',None)
					db.rollback()
					running = False
				elif proc == "DoSelect":
					cb = k.get('callback',None)
					if cb:
						res = 0
						for d in db.DoSelect(a,k):
							cb(d)
							res += 1
					else:
						k['_store'] = True
						res = getattr(db,proc)(*a,**k)
				else:
					res = getattr(db,proc)(*a,**k)
				if d: d.callback(res)
			except Exception as e:
				if d: d.errback()
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
		d.addCallbacks(self.commit,self.rollback)
		
	def _do(self,job,*a,**k):
		d = Deferred()
		debug("QUEUE",id(self.q),job,a,k)
		self.q.put((d,job,a,k))
		return d
		
	def Do(self,*a,**k):
		return self._do("Do",*a,**k)
	def DoFn(self,*a,**k):
		return self._do("DoFn",*a,**k)
	def DoSelect(self,*a,**k):
		return self._do("DoSelect",*a,**k)

