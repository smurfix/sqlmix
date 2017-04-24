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
from traceback import print_exc
from zope.interface import implements
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.defer import Deferred,DeferredList,maybeDeferred,inlineCallbacks,returnValue,succeed
from twisted.python import log
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool
from twisted.internet import threads
from threading import Lock
from Queue import Queue

__all__ = ('DbPool','NoData','ManyData')

_DEBUG = False

NoData = sqlmix.NoData
ManyData = sqlmix.ManyData

def _call(r,p,a,k):
	"""Drop the first argument (i.e. lose the Deferred result)"""
	return p(*a,**k)

def tname():
	import threading
	try:
		return threading._active[threading._get_ident()].name
	except KeyError:
		return "???"

def _print_error(f):
	f.printTraceback(file=sys.stderr)

def debug_(flag,*a):
	if not flag: return
	def pr(x):
		if isinstance(x,(tuple,list)):
			return "\n".join((pr(y).strip("\n") for y in x))
		elif isinstance(x,(basestring,int)):
			try:
				return unicode(x)
			except:
				return repr(x)
		else:
			return repr(x)
	s=" ".join((pr(x) for x in a))

	if flag is True: fl=""
	else: fl=flag+": "
	sys.stderr.write(fl+s+"\n")
	sys.stderr.flush()
def debug(*a):
	debug_(_DEBUG,*a)

class DbPool(object,service.Service):
	"""\
	Manage a pool of database connections.

	TODO: shrink the pool.
	TODO: issue periodic keepalive requests.
	"""
	timeout = 70 # one minute plus
	implements(service.IService)

	def __init__(self,*a,**k):
		"""\
		Create a pool of database connections, for processing (a sequence of)
		SQL commands in the background.
		"""
		k['_single_thread'] = True

		self.db = []
		self.args = a
		self.kwargs = k
		self.lock = Lock()
		self.cleaner = None
		self._tb = {}
		self.stopping = False
		self.threads = ThreadPool(minthreads=2, maxthreads=100, name="Database")
		self.threads.start()
		#reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
		reactor.addSystemEventTrigger('after', 'shutdown', self._dump)
		reactor.addSystemEventTrigger('after', 'shutdown', self.stop2)

	def stop2(self):
		if self.db is not None:
			for db in self.db:
				db[0].close("AfterShutdown Service")
		self.threads.stop()

	def stop(self):
		self.stopping = True

	def _get_db(self,tid=None):
		if self.db:
			r = self.db.pop()[0]
			s="OLD"
		else:
			r = _DbThread(self)
			s="NEW"

		if tid:
			debug(s, r.tid,tid)
			r.tid=tid
		else:
			debug(s, r.tid)

		return r

	def _put_db(self,db):
		if self.db is None or self.stopping:
			db.close("Shutdown")
			return db.done
		if db.q is None:
			raise RuntimeError("Queueing closed DB handle")
		for d in self.db:
			if db is d[0]:
				raise RuntimeError("DoubleQueued")
		db.count = 0
		try:
			t = time()+self.timeout
			self.db.append((db,t))
			if self.cleaner is None:
				self.cleaner = reactor.callLater(self.timeout,self._clean)
		except Exception:
			print_exc()
		else:
			debug("BACK",db.tid)
	
	def _clean(self):
		self.cleaner = None
		t = time()
		while self.db and self.db[0][1] <= t:
			db = self.db.pop(0)[0]
			db.close("Timeout")
		if self.db:
			self.cleaner = reactor.callLater(self.db[0][1]-t,self._clean)
	def __del__(self):
		if self.cleaner:
			reactor.cancelCallLater(self.cleaner)
			self.cleaner = None
		while self.db:
			db = self.db.pop(0)[0]
			db.close("Nonref Parent")

	def stopService(self):
		super(DbPool,self).stopService()
		if self.cleaner:
			self.cleaner.cancel()
			self.cleaner = None
		dbl = self.db
		self.db = None
		dl = []
		for db in dbl:
			db = db[0]
			db.close("Shutdown Service")
			dl.append(db.done)
		return DeferredList(dl)
		
	def __call__(self, job=None,retry=0):
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
		to use the database connection within the block. Otherwise, control
		will have left the "with" block and the connection will be dead.

		Alternately, you can pass a procedure and an optional repeat count:
		>>> def proc(db):
		>>>     d = db.Do("...")
		>>>     return d
		>>> d = dbpool(proc, 10)

		The procedure will be retried up to 10 times if there are errors;
		if they persist, the first error will be re-raised.

		"""
		if not job:
			return self._get_db()
		return self._call(job,retry)

	@inlineCallbacks
	def _call(self, job, retry):
		global tid
		tid += 1
		mtid = tid
		debug("STARTCALL",job,retry,mtid)

		e1 = None
		try:
			while True:
				db = self._get_db(mtid)
				self._note(db)
				try:
					debug("CALL JOB",mtid)
					d = job(db)
					debug("RET JOB",mtid,d)
					def pr(r):
						debug("RES JOB",mtid,r)
						return r
					d.addBoth(pr)
					res = yield d
				except (EnvironmentError,NameError):
					self._denote(db)
					yield db.rollback()
					raise
				except Exception:
					self._denote(db)
					yield db.rollback()
					if retry:
						retry -= 1
						continue
					raise
				except BaseException:
					self._denote(db)
					yield db.rollback()
					raise
				else:
					self._denote(db)
					if isinstance(res,BaseException):
						yield db.rollback()
						if isinstance(res,(EnvironmentError,NameError)):
							returnValue( res )
						elif isinstance(res,Exception):
							if retry:
								retry -= 1
								continue
							returnValue( res )
						else: # BaseException
							returnValue( res )
					else:
						yield db.commit()
					returnValue( res )
		finally:
			debug("ENDCALL",job,retry)

	def _note(self,x):
		if not _DEBUG: return
		import inspect
		self._tb[x.tid] = inspect.stack(1)
	def _denote(self,x):
		if not _DEBUG: return
		del self._tb[x.tid]
	def _dump(self):
		if not _DEBUG: return
		for a,b in self._tb.items():
			#(<frame object at 0x8a1b724>, '/mnt/daten/src/git/sqlmix/sqlmix/twisted.py', 250, '_note', ['\t\tself._tb[x.tid] = inspect.stack(1)\n'], 0)

			print >>sys.stderr,"Stack",a
			for fr,f,l,fn,lin,lini in b[::-1]:
				if fn == "__call__": break
				print >>sys.stderr,"Line %d in %s: %s" % (l,f,fn)
				print >>sys.stderr,"\t"+lin[lini].strip()

def _do_callback(tid,d,res):
	debug("DO_CB",tid,d,res)
	d.callback(res)
	debug("DID_CB",tid,d,res)

tid = 0
class _DbThread(object):
	def __init__(self,parent):
		global tid
		tid += 1
		self.tid = tid
		self.parent = parent
		self.q = Queue()
		debug("INIT",self.tid)
		self.done = threads.deferToThreadPool(reactor, self.parent.threads, self.run,self.q)
		self.started = False
		self.count = 0

		self.committed = []
		self.rolledback = []

	def __repr__(self):
		return "<_DbT.%d>"%(self.tid,)

	def __enter__(self):
		self.parent._note(self)
		return self
	def __exit__(self, a,b,c):
		self.parent._denote(self)
		if self.q is None:
			return False
		if b is None or isinstance(b,sqlmix.CommitThread):
			d = self.commit()
		else:
			from traceback import format_exception
			debug("EXIT ON ERROR",format_exception(a,b,c))
			d = self.rollback()
		d.addErrback(log.err)
		return False

	def call_committed(self,proc,*a,**k):
		self.committed.append((proc,a,k))
	def call_rolledback(self,proc,*a,**k):
		self.rolledback.append((proc,a,k))

	def _run_committed(self,r):
		return self._run_(r,self.committed,"COMMIT")
	def _run_rolledback(self,r):
		return self._run_(r,self.rolledback,"ROLLBACK")
	def _run_(self,r,rc,rcname):
		self.committed = []
		self.rolledback = []
		d = succeed(None)
		for proc,a,k in rc[::-1]:
			debug("AFTER "+rcname,proc,a,k)
			d.addCallback(_call,proc,a,k)
			d.addErrback(log.err)
		d.addCallback(lambda _: r)
		return d


	def run(self,q):
		try:
			db = sqlmix.Db(*self.parent.args,**self.parent.kwargs)
		except BaseException:
			"""No go. Return that error on every call."""
			f = Failure()
			while True:
				d,proc,a,k = q.get()
				if not d: break
				reactor.callFromThread(d.errback,f)
			return
		debug("START",self.tid, tname())
		res = None
		d = True
		while d:
			d = None
			sent = False
			try:
				d,proc,a,k = q.get()
				res = None
				debug("DO",self.tid,d,proc,a,k)
				if proc == "COMMIT":
					db.commit()
					res = k.get('res',None)
				elif proc == "ROLLBACK":
					db.rollback()
					res = k.get('res',None)
				else:
					r = getattr(db,proc)
					debug("CALL",self.tid,r)
					res = r(*a,**k)
					debug("CALLED",self.tid,res)
			except BaseException as e:
				res = Failure()
				if d:
					debug("EB",self.tid,d,res)
					reactor.callFromThread(d.errback,res)
					sent = True
				else:
					debug("ERR",self.tid,res)
					raise
			finally:
				if d and not sent:
					debug("CB",self.tid,d,res)
					reactor.callFromThread(_do_callback,self.tid,d,res)
				debug("DID",self.tid,proc)
		db.close()
		debug("STOP",self.tid)
		return

	def close(self,reason="???"):
		if self.q is None:
			if reason != "__del__":
				debug("DEAD_CALLED_TWICE",self.tid,reason)
			return
		self.q.put((None,"ROLLBACK",[],{}))
		debug("DEAD",self.tid,reason)
		self.q = None

	def __del__(self):
		self.close("__del__")

	def commit(self,res=None):
		d = Deferred()
		if self.count:
			debug("CALL COMMIT",self.tid,d,res)
			self.q.put((d,"COMMIT",[],{'res':res}))
		else:
			debug("NO CALL COMMIT",self.tid,d,res)
			d.callback(res)
		d.addCallback(self._run_committed)
		d.addErrback(self._run_rolledback)
		d.addBoth(self._done)
		return d

	def rollback(self,res=None):
		d = Deferred()
		if self.q is None:
			d.callback(res)
			return d
		if self.count:
			debug("CALL ROLLBACK",self.tid,d,res)
			self.q.put((d,"ROLLBACK",[],{'res':res}))
		else:
			debug("NO CALL ROLLBACK",self.tid,d,res)
			d.callback(res)
		d.addBoth(self._run_rolledback)
		d.addBoth(self._done)
		return d

	def release(self,d):
		"""Convenience method: add myself to a Deferred to stop processing"""
		d.addCallbacks(self.commit,self.rollback)

	def _done(self,r):
		d = maybeDeferred(self.parent._put_db,self)
		d.addCallbacks(lambda _: r, lambda _: _)
		return d

	def _do(self,job,*a,**k):
		"""Wrapper for calling the background thread."""
		self.count += 1
		debug = k.get("_debug",_DEBUG)
		d = Deferred()
		debug_(debug,"QUEUE",self.tid,job,a,k)
		self.q.put((d,job,a,k))
		def _log(r):
			debug_(debug,"DEQUEUE",self.tid,r)
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
	DoSelect.__doc__ = sqlmix.Db.DoSelect.__doc__ + "\nReturns a Deferred.\n"

