# -*- coding: utf-8 -*-

from __future__ import generators,absolute_import

"""\
This class is an anyio-compatible frontend to sqlmix.Db.

It has the same interface, except that all Do* methods return a future.
Internally it works by wrapping trio-mysql or aiopg (asyncio backend only).

 >> import sqlmix.async_ as sqlmix
 >> dbi = sqlmix.DbPool([args of sqlmix.Db])
 >>
 >> async def foo(what_id):
 >>     async with dbi() as db:
 >>         await db.Do("delete from whatever where id=${id}", id=what_id, _empty=1)
 >>         async for x in db.DoSelect("select id from whatever", _empty=1):
 >>            print res
 >>         await db.commit()
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
from sqlmix import NoData,ManyData,fixup_error

from time import time,sleep
import string
import re
import sys
from traceback import print_exc
from contextlib import asynccontextmanager

import anyio

import logging
logger = logging.getLogger(__name__)

__all__ = ('DbPool','NoData','ManyData')

_DEBUG = False

def _call(r,p,a,k):
    """Drop the first argument (i.e. lose the Deferred result)"""
    return p(*a,**k)

def _print_error(f):
    f.printTraceback(file=sys.stderr)

def debug_(flag,*a):
    if not flag: return
    def pr(x):
        if isinstance(x,(tuple,list)):
            return "\n".join((pr(y).strip("\n") for y in x))
        elif isinstance(x,bytes):
            try:
                return x.decode("utf-8")
            except Exception:
                return repr(x)
        elif isinstance(x,str):
            return x
        elif isinstance(x,(float,int)):
            return str(x)
        else:
            return repr(x)
    s=" ".join((pr(x) for x in a))

    if flag is True: fl=""
    else: fl=flag+": "
    logger.debug("%s%s",fl,s)

def debug(*a):
    debug_(_DEBUG,*a)

#class db_data(object):
#    sequential = False
#    _store = 1 # safe default
#    _cursor = True
#    def __init__(self, **kwargs):
#        """standard keywords: host,port,database,username,password"""
#        for f in "host port database username password".split():
#            try:
#                v = kwargs[f]
#            except KeyError:
#                pass
#            else:
#                if f == "port":
#                    v=int(v)
#                if v is not None:
#                    setattr(self,f,v)

class ConnEvt:
    scope=None
    db=None

    def __init__(self):
        self.evt = anyio.Event()

    def set(self, db):
        self.db = db
        self.evt.set()

    async def wait(self):
        await self.evt.wait()

class _db_mysql(sqlmix.db_data):
    port=3306
    def __init__(self, **kwargs):
        self.DB = __import__("trio_mysql")
        super().__init__(**kwargs)
        self.DB.cursors = __import__("trio_mysql.cursors").cursors
        self.DB.paramstyle = 'format'

    async def _conn(self, evt):
        with anyio.CancelScope(shield=True) as sc:
            evt.scope=sc
            async with self.DB.connect(db=self.database, host=self.host, user=self.username, password=self.password, port=self.port, **self.kwargs) as conn:
                evt.set(conn)
                conn._sqlmix_scope = sc
                await anyio.sleep_forever()

    def conn(self, db):
        evt = ConnEvt()
        db._tg.start_soon(self._conn, evt)
        return evt


class _db_postgres(sqlmix.db_data):
    def __init__(self, **kwargs):
        self.DB = __import__("aiopg")
        super().__init__(**kwargs)

    async def conn(self, db):
        res = await self.DB.connect(self.database)
        res._sqlmix_scope = None
        return res

_databases = {
    "mysql": _db_mysql,
    "postgres": _db_postgres,
}

class CtxObj:
    __ctx = None
    async def __aenter__(self):
        if self.__ctx is not None:
            raise RuntimeError("duplicate context")
        self.__ctx = ctx = self._ctx()  # pylint: disable=E1101,W0201
        return await ctx.__aenter__()

    def __aexit__(self, *tb):
        ctx,self.__ctx = self.__ctx,None
        return ctx.__aexit__(*tb)

class Db(CtxObj, sqlmix.DbPrep):
    """\
    Manage a pool of database connections.
    """
    timeout = 70 # one minute plus
    cleaner = None
    _trace = None
    db = None
    id_seq = 0

    def __init__(self,cfg=None, dbtype='mysql', _timeout=None, **kwargs):
        """\
        Create a pool of database connections, for processing (a sequence of)
        SQL commands.
        """

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

        if _timeout is not None:
            self.timeout = _timeout

        kwargs.setdefault('use_unicode',True)
        # kwargs.setdefault('no_delay',True)

        self.kwargs = kwargs

        dbtype = kwargs.pop('dbtype','mysql')
        self.DB = _databases[dbtype](**kwargs)
        self.DB.dbtype=dbtype
        if self._trace is not None:
            self._trace("INIT",dbtype,kwargs)

        self.CArgs = ()

        if hasattr(self.DB,'paramstyle'):
            paramstyle = self.DB.paramstyle
        else:
            paramstyle = self.DB.DB.paramstyle
        (self.arg_init, self.arg_do, self.arg_done) \
            = sqlmix._parsers[paramstyle]

        self.db = []
        self.cleaner = None
        self.stopping = False

        super(Db,self).__init__()

    @asynccontextmanager
    async def _ctx(self):
        async with anyio.create_task_group() as self._tg:
            await self._tg.start(self._clean)
            try:
                yield self
            finally:
                self.close()


    def stop(self):
        self.stopping = True

    @asynccontextmanager
    async def db(self):
        res = await self._get_db()
        try:
            yield res
        finally:
            self._put_db(res)

    async def _get_db(self):
        if self.db:
            r = self.db.pop()[0]
            s="OLD"
        else:
            evt = self.DB.conn(self)
            try:
                await evt.wait()
            except BaseException as e:
                if evt.scope is not None:
                    evt.scope.cancel()
                # otherwise let's hope that the job won't run
                raise
            else:
                r = evt.db
            s="NEW"

        debug(s)
        return r

    def _put_db(self,db):
        if self.db is None or self.stopping:
            db._sqlmix_scope.cancel()
            return
        t = time()+self.timeout
        self.db.append((db,t))
        debug("BACK",getattr(db,"tid",None))
    
    async def _clean(self, task_status):
        with anyio.CancelScope() as self.cleaner:
            task_status.started()
            while True:
                if not self.db:
                    await anyio.sleep(self.timeout)
                    continue
                t = time()
                if self.db[0][1] <= t:
                    db = self.db.pop(0)[0]
                    db.close()
                    continue
                await anyio.sleep(self.db[0][1]-t)

    def close(self):
        self.stopping = True
        if self.cleaner:
            self.cleaner.cancel()
            self.cleaner = None
        while self.db:
            db = self.db.pop(0)[0]
            db._sqlmix_scope.cancel()
        self._tg.cancel_scope.cancel()

    def __call__(self, job=None,retry=0):
        """\
        Get a new connection from the database pool (or start a new one)
        and return a handler object.

        Usage:
        >>> async def something(...):
        >>>     dbpool = Db(...) # arguments like sqlmix.Db()
        >>>     async with dbpool() as db:
        >>>         d = await db.Do("...")
        >>>         do_whatever(d)

        The transaction will be committed if you leave the "with" block
        normally or with a CommitThread exception. Otherwise, it will be
        rolled back.

        Alternately, you can pass a procedure and an optional repeat count:
        >>> def proc(db):
        >>>     d = db.Do("...")
        >>>     return d
        >>> d = await dbpool(proc, 10)

        The procedure will be retried up to 10 times if there are errors;
        if they persist, the first error will be re-raised.

        """
        if job is None:
            if retry:
                raise RuntimeError("You can't use 'retry' without something to call")
            return DbConn(self)
        return self._call(job,retry)

    async def _call(self, job, retry):
        global tid
        tid += 1
        mtid = tid
        err = None
        debug("STARTCALL",job,retry,mtid)

        e1 = None
        try:
            while True:
                db = self._get_db()
                self._note(db)
                try:
                    debug("CALL JOB",mtid)
                    d = await job(db)
                    debug("RET JOB",mtid,d)
                    def pr(r):
                        debug("RES JOB",mtid,r)
                        return r
                    return d
                except (EnvironmentError,NameError) as e:
                    self._denote(db)
                    await db.rollback()
                    raise
                except Exception as e:
                    self._denote(db)
                    await db.rollback()
                    if e1 is None:
                        e1 = e
                    if retry:
                        retry -= 1
                        continue
                    raise e1
                except BaseException:
                    self._denote(db)
                    await db.rollback()
                    raise
                else:
                    self._denote(db)
                    await db.commit()
                    return res
        finally:
            debug("ENDCALL",job,retry)

    def _note(self,x):
        if not _DEBUG: return
        import inspect
        self._tb[x.tid] = inspect.stack(1)
    def _denote(self,x):
        if not _DEBUG: return
        del self._tb[x.tid]

    async def Do(self,cmd,**kv):
        async with self() as db:
            return await db.Do(cmd, **kv)

    async def DoFn(self,cmd,**kv):
        async with self() as db:
            return await db.DoFn(cmd, **kv)

    async def DoSelect(self,cmd,**kv):
        n = 0
        async with self() as db:
            async for r in db.DoSelect(cmd,**kv):
                yield r

def _do_callback(tid,d,res):
    debug("DO_CB",tid,d,res)
    d.callback(res)
    debug("DID_CB",tid,d,res)

class DbConn(CtxObj):
    """\
    Manage a single connection.
    """
    curs = None
    db = None
    work = 0

    def __init__(self,pool):
        self.pool = pool
        self.committed = []
        self.rolledback = []
        self._trace = pool._trace
        pool.id_seq += 1
        self.id = pool.id_seq

    @asynccontextmanager
    async def _ctx(self):
        assert self.db is None
        assert self.curs is None
        self.db = await self.pool._get_db()
        self.DB = self.pool.DB
        try:
            async with self.db.cursor():
                try:
                    yield self

                except sqlmix.CommitThread:
                    pass
                except Exception as exc:
                    from traceback import format_exception
                    debug("ERROR",format_exception(exc))
                    await self.rollback()
                    raise
                try:
                    await self.commit()
                except sqlmix.CommitThread:
                    pass
                except Exception as exc:
                    await self.rollback()
                    raise
        except Exception:
            self.pool._put_db(self.db)
            raise
        except BaseException:
            self.db._sqlmix_scope.cancel()
            raise
        else:
            self.pool._put_db(self.db)
        finally:
            self.db = None


    def call_committed(self,proc,*a,**k):
        self.committed.append((proc,a,k))
    def call_rolledback(self,proc,*a,**k):
        self.rolledback.append((proc,a,k))

    def _run_committed(self):
        return self._run_(self.committed, "COMMIT")
    def _run_rolledback(self):
        return self._run_(self.rolledback, "ROLLBACK")
    async def _run_(self,rc,rcname):
        self.committed = []
        self.rolledback = []
        for proc,a,k in rc[::-1]:
            debug("AFTER "+rcname,proc,a,k)
            try:
                await _call(proc,a,k)
            except Exception as exc:
                logger.exception(proc)

    def close(self,reason="???"):
        debug("CLOSE",self.id)
        if self.curs is not None:
            self.curs.close()
            self.curs = None
        sc = self.db._sqlmix_scope
        if sc is not None:
            self.db._sqlmix_scope = None
            sc.cancel()

    async def commit(self,res=None):
        if self.work == 0:
            return
        debug("COMMIT",self.id)
        await self.db.commit()
        self.work = 0
        await self._run_committed()

    async def rollback(self,res=None):
        if self.work == 0:
            return
        debug("ROLLBACK",self.id)
        await self.db.rollback()
        self.work = 0
        await self._run_rolledback()

    async def _cursor(self, cmd, **kv):
        cmd = self.pool.prep(cmd, **kv)
        try:
            async with self.db.cursor() as curs:
                await curs.execute(*cmd)
        except:
            fixup_error(cmd)
            raise
        return curs
        
    async def DoFn(self, cmd, **kv):
        debug("DOFN",self.id,cmd,kv)
        self.work += 1
        curs = await self._cursor(cmd, **kv)

        if hasattr(curs,'fetchone'):
            val = await curs.fetchone()
        elif not curs.rows:
            val = None
        else:
            val = curs.rows.pop(0)

        if self._trace is not None:
            self._trace("DoFn",cmd,val)
        if not val:
            raise NoData(cmd,kv)

        as_dict=kv.get("_dict",None)
        if as_dict:
            if as_dict is True:
                as_dict = dict
            names = map(lambda x:x[0], curs.description)

        if ((await curs.fetchone()) is not None) if hasattr(curs,'fetchone') else curs.rows:
            raise ManyData(cmd)
        if self.curs is None:
            await curs.aclose()

        if as_dict:
            val = as_dict(zip(names,val))
        return val

    async def Do(self, cmd, **kv):
        """Database-specific Do function"""
        debug("DO",self.id,cmd,kv)
        self.work += 1
        curs = await self._cursor(cmd, **kv)

        r = curs.lastrowid
        if not r:
            r = curs.rowcount
        if self.curs is None:
            await curs.aclose()

        if self._trace is not None:
            self._trace("Do",cmd,r)
        if r == 0 and not kv.get('_empty', False):
            raise NoData(cmd, kv)
        return r

    async def DoSelect(self, cmd, **kv):
        """Database-specific DoSelect function"""
        debug("DOSEL",self.id,cmd,kv)
        self.work += 1
        curs = await self._cursor(cmd, **kv)

        n = 0
        as_dict=kv.get("_dict",None)
        if as_dict is True:
            as_dict = dict
        if as_dict:
            names = list(map(lambda x:x[0], curs.description))

        try:
            while True:
                if hasattr(curs,'fetchone'):
                    val = await curs.fetchone()
                    if val is None:
                        break
                elif not curs.rows:
                    break
                else:
                    val = curs.rows.pop(0)

                if as_dict:
                    val = as_dict(zip(names,val))

                n += 1
                yield val

        finally:
            if self._trace is not None:
                self._trace("DoSel",_cmd,val)
            if self.curs is None:
                with anyio.move_on_after(3, shield=True):
                    await curs.aclose()
        if n == 0 and not kv.get('_empty', False):
            raise NoData(cmd, kv)

    Do.__doc__ = sqlmix.Db.Do.__doc__ + "\nReturns a Future.\n"
    DoFn.__doc__ = sqlmix.Db.DoFn.__doc__ + "\nReturns a Future.\n"
    DoSelect.__doc__ = sqlmix.Db.DoSelect.__doc__ + "\nReturns a Future.\n"

