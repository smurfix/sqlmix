#!/usr/bin/python
# -*- coding: utf-8 -*-

#    Copyright (C) 2008-2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

"""\
Transform SQL tables.
This tool analyzes SQL table definitions+contents and emits mostly-minimal
statements to transform one into the other.

For content analysis, an ID column is mandatory; a timestamp column is
strongly recommended.

This code needs to be preprocessed with yapps2 (a Python parser generator).
Specifically, with version 2.1.1 from Debian/Ubuntu, because
that contains a couple of crucial enhancements.

"""

import sys, os, copy, time, array
import MySQLdb.connections as mc
from traceback import format_exception_only

class DupTableError(ReferenceError):
	pass
class DupFieldError(ReferenceError):
	pass
class DupKeyError(ReferenceError):
	pass
class NoSuchTableError(IndexError):
	pass

verbose=1
_debug=None
keyseq = 0
had_output = 0

r="\r"+(" "*70)+"\r"
INTs = ("int","smallint","bigint","tinyint","mediumint")
FLOATs = ("float","double")

# Update-Trace
cnt=0
cntt = ""
ltm=int(time.time())-1

def bq(n):
	"""Backquote table/field names"""
	# We might be nice for readability and only quote keywords.
	# However, right now we don't bother.
	return '`'+n+'`'
			
def sqlquote(s):
	if not isinstance(s,basestring):
		s = str(s)
	def _prep(p):
		if p == "\0": return "\\0"
		else: return "\\"+p
	s = re.sub("(\\'\0)",_prep,s)
	return "'"+s+"'"

def print_query(d):
	res = ""
	resl={}
	for fd,val in d.items():
		if res != "": res += " and "
		res += bq(fd)+"="
		if val is None:
			res += "NULL"
		else:
			res += '${%s}' % (fd,)
			resl[fd]=val
	return (res,resl)
	
def print_update(d):
	res = ""
	resl={"_debug":_debug}
	for fd,val in d.items():
		if res: res += ", "
		res += bq(fd)+"="

		if val is None:
			res += "NULL"
		else:
			if not isinstance(val,array.array):
				val=str(val)
			res += '${%s_}' % (fd,)
			resl[fd+'_']=val

	return (res,resl)

def print_insert(d):
	res = ""
	resg = ""
	resl={"_debug":_debug}
	for fd,val in d.items():
		if res:
			res += ", "
			resg += ", "
		res += bq(fd)
		if val is None:
			resg += "NULL"
		else:
			if not isinstance(val,array.array):
				#try: val=int(val)
				#except OverflowError: val=float(val)
				#except ValueError: pass
				#except TypeError: val=str(val)
				val=str(val)
			resg += '${%s_}' % (fd,)
			resl[fd+'_']=val
	return ('(%s) VALUES (%s)' % (res,resg), resl)

def posval2dict(fieldpos,ar,br=None):
	d = {}
	for fd,idx in fieldpos.items():
		val = ar[idx]
		if br:
			if val == br[idx]: continue
		d[fd] = val
	return d
	

def trace(*x):
	if not verbose: return
	print >>sys.stderr, r+"  "+" ".join(map(str,x))+"   \r",
	sys.stderr.flush()

def tdump(n):
	if n.__class__ == ().__class__:
		r=""
		for f in n: r += "'%s', " % (f,)
		return "ENUM ("+r[:-2]+")"
	else:
		return n

class Uncommon(object): pass
def common(d1,*d):
	"""
	Return a dict with only those keys/values common to all.
	
	The first argument needs to be a dict, others can also be lists.
	If a list/tuple is passed, filter by keys.

	>>> common({1:2,3:4,5:6,7:8},{1:3,5:6,7:8,9:10},[1,3,5,9])
	{5:6}

	"""
	d1orig = d1
	for d2 in d:
		dn = {}
		if isinstance(d2,(list,tuple)):
			for k in d2:
				try:
					dn[k] = d1[k]
				except KeyError:
					pass # just ignore
		else:
			for k,v in d1.items():
				if d2.get(k,Uncommon) == v:
					dn[k]=v
		d1 = dn
	return d1

def notcommon(d1,*d):
	"""
	Return a dict with only those keys/values in the first dict that are
	different from (or not present in) the rest.

	>>> common({1:2,3:4,5:6,7:8},{1:3,5:6,7:8,9:10})
	{1:2}

	"""
	res = dict(d1)
	for d2 in d:
		if isinstance(d2,(list,tuple)):
			for k in d2:
				try:
					del res[k]
				except KeyError:
					pass # just ignore
		else:
			for k,v in res.items():
				if d2.get(k,Uncommon) == v:
					del res[k]
	return res


def print_fkey_check(what=0):
	global had_output
	if had_output == what:
		had_output=1
		print "SET FOREIGN_KEY_CHECKS = ",what,";"

class curtime: pass
class NotGiven: pass

def valprint(x):
	if x is None: return "NULL"
	if x is curtime: return "CURRENT_TIMESTAMP"
	return "'"+str(x)+"'" ## TODO: quote correctly

class Field(object):
	"""Declare a table's column"""
	def __init__(self,table,name,after=True):
		self.name=name
		self.old_name=name
		self.table=table
		self.tname=None
		self.next_col=None
		self.prev_col=None

		self.nullable=True
		self.len=None
		self.len2=None
		self.defval=NotGiven
		self.updval=NotGiven
		self.autoinc=False
		self.unsigned=False
		self.charset=None
		self.collation=None

		self._chained=False
		self._tagged=False
		table._new_field(self,after=after)

	def clone(self,table):
		f = Field(table,self.name)
		f.tname=self.tname
		f.nullable=self.nullable
		f.len=self.len
		f.len2=self.len2
		f.defval=self.defval
		f.updval=self.updval
		f.autoinc=self.autoinc
		f.unsigned=self.unsigned
		f.charset=self.charset
		f.collation=self.collation
		table._new_field(f)

	def create_done(self):
		"""Fill in some defaults"""
		if self.tname == 'bool': # mysql specific
			self.tname = 'tinyint'
			self.len = 1
		if self.defval is NotGiven:
			if self.autoinc or self.nullable:
				self.defval = None
			elif self.tname in INTs or self.tname in FLOATs:
				self.defval = "0"
			elif self.tname == "datetime":
				self.defval = "0000-00-00 00:00:00"
			else:
				self.defval = ""
		if self.len is None:
			if self.tname == "char":
				self.len = 1
			elif self.tname == "tinyint":
				self.len = 4
			elif self.tname == "smallint":
				self.len = 6
			elif self.tname == "mediumint":
				self.len = 9
			elif self.tname == "int":
				self.len = 11
			elif self.tname == "bigint":
				self.len = 20
			elif self.tname in INTs:
				raise RuntimeError("INT problem",self.tname)

			if self.unsigned and self.len and self.len != 20:
				self.len -= 1
		if self.tname == "timestamp" and self.defval is curtime \
				and not self.nullable:
			self.nullable = True
		if self.tname in INTs \
				and self.autoinc and self.nullable:
			self.nullable = False

	def drop(self):
		self.table._del_field(self)
	
	def dump(s,*f,**k):
		"""Dump myself, either completely or as a diff to column 'oc'"""
		a,b=s.dump2(*f,**k)
		return a

	def is_text_column(self):
		return (self.tname == "char" or self.tname == "varchar" or self.tname.endswith("text"))

	def dump2(self,oc=None,in_create=False,skip_flags=""):
		"""Dump myself, also return changes"""
		if not self.charset: self.charset=self.table.charset
		if not self.collation: self.collation=self.table.collation
		if oc:
			if not oc.charset: oc.charset=oc.table.charset
			if not oc.collation: oc.collation=oc.table.collation
		if not in_create:
			if not oc:
				r="ADD COLUMN "
			elif not self._diffs(oc,skip_flags=skip_flags):
				return None,None
			elif self.name != oc.name:
				r="CHANGE COLUMN `%s` " % (oc.name,)
			else: # MySQL Special ??
				r="MODIFY COLUMN "
		else:
			r=""
		rp=[]
		r += "`%s` %s" % (self.name,tdump(self.tname))
		if self.len is not None:
			if self.len2 is not None:
				r += "(%d,%d)" % (self.len,self.len2)
			else:
				r += "(%d)" % (self.len,)
		if oc: 
			if self.tname != oc.tname: rp.append(oc.tname)
			if self.len != oc.len or self.len2 != oc.len2:
				if oc.len is not None: rp.append("len=%d"%(oc.len,))
				else: rp.append("!len")

		if self.unsigned:
			r += " UNSIGNED"
		if oc:
			if self.unsigned != oc.unsigned:
				if oc.unsigned: rp.append("unsigned")
				else: rp.append("signed")

		do_cs=False
		if self.is_text_column():
			if 'c' not in skip_flags and self.charset:
				if oc and oc.charset == "utf8":
					charset = oc.charset
				else:
					charset = self.charset
				if charset != self.table.charset or charset != self.charset or (oc and (charset != oc.charset or charset != oc.table.charset)):
					r += " CHARACTER SET %s" % (charset,)
					do_cs=True
			if 'c' not in skip_flags and oc:
				if self.charset != oc.charset:
					rp.append("charset %s/%s"%(oc.charset,self.charset))
	
			if 'c' not in skip_flags and self.collation and not do_cs:
				if oc and oc.charset == "utf8" and oc.charset != self.charset:
					collation = oc.collation
				else:
					collation = self.collation
				r += " COLLATE %s" % (collation,)
			if 'c' not in skip_flags and oc and not do_cs:
				if oc.collation and (self.collation != oc.collation):
					rp.append("collate %s/%s"%(oc.collation,self.collation))
	
		if self.nullable:
			if not in_create:
				r += " NULL" # ist eh Default
		else:
			r += " NOT NULL"
		if oc:
			if self.nullable != oc.nullable:
				if oc.nullable: rp.append("null")
				else: rp.append("!null")

		if self.autoinc:
			r += " AUTO_INCREMENT"
		if oc:
			if self.autoinc != oc.autoinc:
				if oc.autoinc: rp.append("autoinc")
				else: rp.append("!autoinc")

		if self.defval is not NotGiven:
			r += " DEFAULT " + valprint(self.defval)
		if oc:
			if self.defval != oc.defval:
				if oc.defval != NotGiven: rp.append("default "+valprint(oc.defval))
				else: rp.append("!default")

		if self.updval is not NotGiven:
			r += " ON UPDATE " + valprint(self.updval)
		if oc:
			if self.updval != oc.updval:
				if oc.updval != NotGiven: rp.append("updefault "+valprint(oc.updval))
				else: rp.append("!updefault")

		if 'a' not in skip_flags and oc and self.prev_col != oc.prev_col and not in_create:
			if self.prev_col:
				r += " AFTER %s" % (self.prev_col.name,)
			else:
				r += " FIRST"
		if 'a' not in skip_flags and oc:
			if self.prev_col != oc.prev_col:
				if oc.prev_col: rp.append("after "+oc.prev_col.name)
				else: rp.append("first")

		return r,rp

	def _diffs(s,o,skip_flags=""):
		"""return TRUE if s and o differ"""
		if 'a' not in skip_flags:
			if s.prev_col: pa=s.prev_col.name
			else: pa=None
			if o.prev_col: pb=o.prev_col.name
			else: pb=None
			if pa != pb: return True
		if s.name != o.name: return True
		if s.tname != o.tname: return True
		if s.len != o.len or s.len2 != o.len2:
			if s.tname != "timestamp" or (s.len is not None and o.len is not None):
				return True
		if s.autoinc != o.autoinc: return True
		if s.unsigned != o.unsigned: return True
		if s.nullable != o.nullable: return True
		if s.is_text_column():
			if 'c' not in skip_flags and s.charset != o.charset and (s.charset == "utf8" or o.charset != "utf8"):
				return True
			if 'c' not in skip_flags and s.collation != o.collation:
				if o.collation is not None and s.collation is not None and s.collation != o.collation and (s.charset == "utf8" or o.charset != "utf8"):
					return True
		if s.defval != o.defval:
			if s.tname != "timestamp" or (s.defval is not NotGiven and s.defval is not curtime) or (o.defval is not NotGiven and o.defval is not curtime):
				return True
		if s.updval != o.updval:
			if s.tname != "timestamp" or (s.updval is not NotGiven and s.updval is not curtime) or (o.updval is not NotGiven and o.updval is not curtime):
				return True
		# ignore: next_col
		return False

	def __hash__(self): return self.old_name.__hash__() # is immutable
	def __str__(self): return "<%s:%s %s>" % (self.name,self.tname, self.table)
	def __repr__(self): return "<%s.%s>" % (self.table.name,self.name)

	def set_after(self,after=True,oc=None):
		self.table._del_field(self)
		self.table._new_field(self,after=after)
	def __eq__(self,other):
		if other is None: return False
		return self.name.__eq__(other.name)
	def __ne__(self,other):
		if other is None: return True
		return self.name.__ne__(other.name)
	#def __cmp__(self,other): return self.name.__cmp__(other.name)
		
class Key(object):
	def __init__(self,table,name,ktype,fields):
		self.table=table
		self.name=name
		self.ktype=ktype
		self.fields=fields
		self.id = (ktype,fields)

	def pref(self, pref_seq=False, pref_text=True):
		"""Pref value for this key"""
		pr=1000

		if len(self.fields)==1 and self.ktype=="U":
			k = self.fields[0][0]
			if k.autoinc and pref_seq:
				pr *= 10
			if not k.autoinc and not pref_seq:
				pr *= 10
		if self.name is None: # primary key
			pr += 20

		for f in self.fields:
			f = f[0]
			if f.name in ("id","lfd"):
				pr += 50
			if f.tname in INTs:
				if not pref_text:
					pr *= 5
			else:
				if pref_text:
					pr *= 5

		if self.ktype!="U":
			pr /= 10
		return pr/len(self.fields)

	def dump(self,ok=None,in_create=False,skip_flags=""):
		"""Dump myself, either completely or as a diff to key 'ok'"""
		if not in_create:
			if not ok:
				r="ADD"
			elif not self._diffs(ok,skip_flags=skip_flags):
				return None
			else:
				r=""
				if ok.name is None:
					r += "DROP PRIMARY KEY,\n    "
				else:
					r += "DROP KEY `%s`,\n    " % (ok.name,)
				r+="ADD"

		else:
			r=""

		if self.name is None:
			r += " PRIMARY KEY"
		else:
			if self.ktype == "U": r+=" UNIQUE"
			r += " KEY `%s`" % (self.name,)
		r += "("
		ra=""
		for f in self.fields:
			r += ra+"`"+f[0].name+"`"
			if f[1]:
				r += "("+str(f[1])+")"
			ra=","
		r += ")"
		return r

	def _diffs(self,ok,skip_flags=""):
		if self.fields != ok.fields: return True
		if 'i' not in skip_flags and self.name != ok.name: return True
		if self.ktype != ok.ktype: return True
		return False

	def __str__(self):
		n=self.name
		if not n: n="PRIMARY"
		return "<%s %s %s>" % (self.ktype,n,",".join(map(str,self.fields)))
	def __repr__(self):
		n=self.name
		if not n: n="PRIMARY"
		return "<%s %s.%s %s>" % (self.ktype,self.table.name,n,self.fields)


class FKey(object):
	def __init__(self,name,table,fields,rtable,rfields, opt):
		self.name=name
		self.table=table
		self.rtable=rtable
		self.fields=fields
		self.rfields=rfields
		self.opt=opt

	def dump(self,ok=None,in_create=False,skip_flags=""):
		"""Dump myself, either completely or as a diff to key 'ok'"""
		if not in_create:
			if not ok:
				r = "ADD"
			elif not self._diffs(ok,skip_flags=skip_flags):
				return None
			else:
				r = "ADD"

		else:
			r=""

		r += " CONSTRAINT `%s` FOREIGN KEY (" % (self.name,)
		ra=""
		for f in self.fields:
			r += ra+"`"+f.name+"`"
			ra=","
		r += ") REFERENCES `%s` (" % (self.rtable,)
		ra=""
		for f in self.rfields:
			r += ra+"`"+f+"`"
			ra=","
		r += ")"
		for a,b in self.opt.items():
			r += " ON %s %s" % (a.upper(),b)
		return r

	def _diffs(self,ok,skip_flags=""):
		if 'i' not in skip_flags and self.name != ok.name:
			print "#FK Name:",self.name,ok.name
			return True
		if self.rtable != ok.rtable:
			print "#FK",self.name,"rtable:",self.rtable,ok.rtable
			return True
		fs=[f.name for f in self.fields]
		ofs=[f.name for f in ok.fields]
		if fs != ofs:
			print "#FK",self.name,"fields:",fs,ofs
			return True
		if self.rfields != ok.rfields:
			print "#FK",self.name,"rfields:",self.rfields,ok.rfields
			return True
		for a,b in self.opt.items():
			try: c=ok.opt[a]
			except KeyError:
				print "#FK",self.name,"k_opt",a,"??"
				return True
			else:
				if b != c:
					print "#FK",self.name,"k_opt",a,b,c
					return True
		for a,b in ok.opt.items():
			try: c=self.opt[a]
			except KeyError:
				print "#FK",self.name,"ok_opt",a,"??"
				return True
			else:
				if b != c:
					print "#FK",self.name,"ok_opt",a,b,c
					return True
		return False

	def __str__(self): return "<%s %s>" % \
				(",".join([f.name for f in self.fields]),
				 ",".join(self.rfields))
	def __repr__(self): return "<%s.%s %s.%s>" % \
				(self.table, ",".join([f.name for f in self.fields]),
				 self.rtable, ",".join(self.rfields))


class Table(object):
	"""Declare a table"""
	def __init__(self,name,db=None):
		self.name=name
		self.engine=None
		self.charset="utf8"
		self.collation="utf8_general_ci"
		self.packing=None
		self.maxrows=None

		self.col={}
		self.key={}
		self.knames={}
		self.dupkeys=[]

		self.fkey={}
		self.fknames={}
		self.dupfkeys=[]
		self.contents={}

		if db:
			db.tables[name]=self
			self.tables = db.tables

		self.first_col=None
		self.last_col=None

		self.db = db

	def get(self,d):
		"""Retrieve a dataset that has the same key as d"""
		for k in self.possible_keys(d):
			if k in self.contents:
				return self.contents[k]
		return None

	def pop(self,d):
		"""Retrieve a dataset that has the same key as d, and delete it"""
		for k in self.possible_keys(d):
			if k in self.contents:
				dd = self.contents[k]
				for k in self.possible_keys(dd):
					if k in self.contents:
						del self.contents[k]
				return dd
		return None

	def update(self, d):
		#from pprint import pprint
		"""Insert a (possibly incomplete) dataset into this table"""
		#print "UPDATE",self.name,"";pprint(self.contents)
		#print "WITH","",;pprint(d)
		dd = None
		for k in self.possible_keys(d):
			if k in self.contents:
				dd = self.contents[k]
				dd.update(d)
				break
		if not dd:
			# not found ⇒ add (a copy)
			dd = dict(d)
		for k in self.possible_keys(dd):
			self.contents[k] = dd
		#print "GETS","",;pprint(self.contents)
	
	def possible_keys(self, d):
		"""Get all possible keys for a dataset"""
		for bk in self.best_keys(False,True):
			try:
				yield tuple((bk,)+tuple( d[f[0].name] for f in bk.fields ))
			except KeyError:
				pass

	def possible_key(self, d):
		"""Return the best-possible key for a dataset"""
		return self.possible_keys(d).next()


	def clone(self,db):
		t = Table(self.name)
		t.engine = self.engine
		t.charset = self.charset
		t.collation = self.collation
		t.packing = self.packing
		t.maxrows = self.maxrows

		f = self.first_col
		while f:
			f.clone(t)
			f = f.next_col
		t.key.update(self.key)
		t.knames.update(self.knames)
		t.dupkeys = self.dupkeys[:]

		t.fkey.update(self.fkey)
		t.fknames.update(self.fknames)
		t.dupfkeys = self.dupfkeys[:]

		t.first_col = self.first_col
		t.last_col = self.last_col
		t.tables = db.tables
		t.db = db
		return t

	def best_keys(self, pref_seq=True, pref_text=True, only_unique = True):
		"""Returns all indices on the table, sorted by preference."""
		k=self.key.values()
		if only_unique:
			k = [x for x in k if x.ktype == 'U']
		k.sort(lambda a,b: b.pref(pref_seq,pref_text)-a.pref(pref_seq,pref_text))
		return k

	def timestamp(self):
		# Return the column with tiimestamp
		for c in self.col.itervalues(): # new MySQL
			if c.updval == curtime:
				return c
		for c in self.col.itervalues(): # one common possibility
			if c.tname == "timestamp":
				return c
		for c in self.col.itervalues(): # another common possibility
			if c.name == "tstamp":
				return c
		return None
		
	def new_fkey(self,name,fl1,rtable,fl2,opt):
		if name in self.fknames:
			raise DupKeyError(name)
		fl1=tuple(fl1)
		fl2=tuple(fl2)
		try:
			k=self.fkey[fl1]
		except KeyError:
			k=FKey(name,self,fl1,rtable,fl2,opt)
			self.fkey[fl1]=k
			self.fknames[name]=k
			try: self.dupfkeys = self.dupfkeys.remove(name)
			except ValueError: pass
		else:
			if k.name is None: repl=False
			elif name is None: repl=True
			else: repl=False
			if repl: # alten Key rauswerfen
				self.dupfkeys.append(k.name)
				del self.fknames[k.name]

				k.name=name
				self.fknames[name]=k
			else:
				if k.name < name:
					self.dupfkeys.append(name)
				else:
					self.dupfkeys.append(k.name)
					k.name = name
	
	def del_fkey(self,name):
		try:
			k=self.fknames.pop(name)
		except KeyError:
			self.dupfkeys=self.dupfkeys.remove(name)
		else:
			del self.fkey[k.fields]

	def new_key(self,name,ktype,fields):
		if name == "":
			global keyseq
			keyseq += 1
			name = "x_" + str(keyseq)
		if name in self.knames:
			raise DupKeyError(name)
		fields=tuple(fields)
		k=Key(self,name,ktype,fields)
		try:
			k=self.key[k.id]
		except KeyError:
			self.key[k.id]=k
			self.knames[name]=k
			try: self.dupkeys = self.dupkeys.remove(name)
			except ValueError: pass
		else:
			if k.name is None: repl=False
			elif name is None: repl=True
			elif ktype != k.ktype: repl=True
			else: repl=False
			if repl: # kick the old key
				self.dupkeys.append(k.name)
				del self.knames[k.name]

				k.name=name
				self.knames[name]=k
			else:
				if k.name != name: self.dupkeys.append(name)
	
	def del_key(self,name):
		try:
			k=self.knames.pop(name)
		except KeyError:
			self.dupkeys=self.dupkeys.remove(name)
		else:
			del self.key[k.id]

	def drop(self):
		del self.tables[self.name]

	def columns(self):
		c=self.first_col
		while c:
			cn=c.next_col
			yield c
			c=cn

	def new_field(self,name,old_col):
		"""add a new field to this table.
		  (May be a clone of some old field.)"""

		if old_col:
			if old_col is True:
				old_col=name
			old_col=self.col[old_col]
			after = old_col.prev_col
			old_col.drop()
			old_col.name=name
			old_col._chained=False
			self._new_field(old_col,after=after)

			return old_col
		else:
			if name in self.col:
				raise DupFieldError(self,name)
			return Field(self,name)

	def _new_field(self,col,after=True):
		if col._chained: return
		col._chained=True
		self.col[col.name]=col

		if after is None:
			col.prev_col=None
			col.next_col=self.first_col
		elif after is True:
			col.next_col=None
			col.prev_col=self.last_col
		else:
			col.prev_col=after
			col.next_col=after.next_col

		if col.prev_col: col.prev_col.next_col = col
		else: self.first_col=col
		if col.next_col: col.next_col.prev_col = col
		else: self.last_col=col

	def del_field(self,name):
		self.col[name].drop()
	def _del_field(self,col):
		if self.col[col.name] != col: raise IndexError

		if not col._chained: return
		col._chained=False
		del self.col[col.name]

		if col.prev_col: col.prev_col.next_col=col.next_col
		else: self.first_col=col.next_col
		if col.next_col: col.next_col.prev_col=col.prev_col
		else: self.last_col=col.prev_col

	def dump(self,skip_flags=""):
		r= "CREATE TABLE %s (\n    " % (self.name,)
		ra=""
		for c in self.columns():
			r += ra+c.dump(in_create=True,skip_flags=skip_flags)
			ra=",\n    "
		for k in self.key.values():
			r += ra+k.dump(in_create=True,skip_flags=skip_flags)
			ra=",\n    "
		for k in self.fkey.values():
			r += ra+k.dump(in_create=True,skip_flags=skip_flags)
			ra=",\n    "
		r += ")"
		if self.engine:
			r += " ENGINE="+self.engine
		if 'c' not in skip_flags and self.charset:
			r += " CHARACTER SET="+self.charset
		if 'c' not in skip_flags and self.collation:
			r += " COLLATE="+self.collation
		if 'm' not in skip_flags and self.maxrows:
			r += " MAX_ROWS="+self.maxrows
		if 'p' not in skip_flags and self.packing is not None:
			r += " PACK_KEYS="+str(self.packing)
		return r

	def diff1(s,o,skip_flags=""):
		r=""
		ra="ALTER TABLE %s\n    " % (s.name,)

		for on in o.dupkeys:
			if on == "":
				r += ra+"DROP PRIMARY KEY"
			else:
				r += ra+"DROP KEY `%s`" % (on,)
			ra=",\n    "

		if 'k' not in skip_flags:
			for fk in o.fkey.values():
				if fk.name not in s.fknames or s.fknames[fk.name]._diffs(fk,skip_flags=skip_flags):
					r += ra+"DROP FOREIGN KEY `%s`" % (fk.name,)
					ra=",\n    "
		if 'e' not in skip_flags:
			if s.engine and s.engine != o.engine:
				r += ra+" ENGINE="+s.engine
				ra=""
		if 'c' not in skip_flags and s.charset and s.charset != o.charset:
			r += ra+" CHARACTER SET="+s.charset
			ra=""
		if 'c' not in skip_flags and o.collation and s.collation and s.collation != o.collation:
			r += ra+" COLLATE="+s.collation
			ra=""
		if 'm' not in skip_flags and s.maxrows and s.maxrows != o.maxrows:
			r += ra+" MAX_ROWS="+str(s.maxrows)
			ra=""
		if 'p' not in skip_flags and s.packing is not None and s.packing != o.packing:
			r += ra+" PACK_KEYS="+str(s.packing)
			ra=""
		return r

	def diff2(s,o,skip_flags=""):
		r=""
		ra="ALTER TABLE %s\n    " % (s.name,)
		rb=""

		dels = []
		for k in o.key.values():
			if 'i' in skip_flags and k.name is not None:
				if k.id in s.key: continue
			else:
				if k.name in s.knames: continue
			r += ra+"DROP "
			if k.name is None:
				r +="PRIMARY KEY"
			else:
				r += "KEY `%s`" % (k.name,)
			ra=",\n    "
			rb += "# %s \n" % (k,)
			dels.append(k)
		for k in dels:
			del o.key[k.id]
			del o.knames[k.name]

		for k in o.key.values():
			try:
				if 'i' in skip_flags:
					ok=s.key[k.id]
				else:
					ok=s.knames[k.name]
			except KeyError:
				r += ra+"DROP "
				if k.name is None:
					r +="PRIMARY KEY"
				else:
					r += "KEY `%s`" % (k.name,)
			else:
				continue
		# First pass: add new columns
		for c in s.columns():
			nr=""
			try: oc=o.col[c.old_name]
			except KeyError:
				r += ra+c.dump(skip_flags=skip_flags)
				ra=",\n    "
		# Second pass: modify new columns
		for c in s.columns():
			nr=""
			try: oc=o.col[c.old_name]
			except KeyError: continue
			else:
				nr,onr = c.dump2(oc,skip_flags=skip_flags)
				oc._tagged=True
				if nr:
					r += ra+nr
					ra=",\n    "
					rb += "#%s: %s\n" % (c.name,", ".join(onr))
		# Third pass: delete old columns
		for c in o.columns():
			if not c._tagged:
				r += ra+"DROP COLUMN `%s`" % (c.name,)
				ra=",\n    "
			else:
				c._tagged=False

		# Now fix the indices
		for k in s.key.values():
			try:
				if 'i' in skip_flags:
					ok=o.key[k.id]
				else:
					ok=o.knames[k.name]
			except KeyError:
				nr=k.dump(skip_flags=skip_flags)
			else:
				if k._diffs(ok,skip_flags=skip_flags):
					nr=k.dump(ok,skip_flags=skip_flags)
				else:
					nr=""
			if nr:
				r += ra+nr
				ra=",\n    "

		return r,rb

	def diff3(s,o,skip_flags=""):
		r=""
		ra="ALTER TABLE %s\n    " % (s.name,)

		if 'k' not in skip_flags:
			for fk in s.fkey.values():
				try:
					if 'i' in skip_flags:
						ok=o.fkey[fk.fields]
					else:
						ok=o.fknames[fk.name]
				except KeyError:
					nr=fk.dump(skip_flags=skip_flags)
				else:
					if fk._diffs(ok,skip_flags=skip_flags):
						nr=fk.dump(ok,skip_flags=skip_flags)
					else:
						nr=""
				if nr:
					r += ra+nr
					ra=",\n    "
		return r
		
	def __str__(self): return "<%s %s>" % (self.name,self.db)
	__repr__=__str__
	def colnames(self):
		n=[]
		c=self.first_col
		while c:
			n.append(c.name)
			c=c.next_col
		return n
		
dbid = 0

class Schema:
	# self.tables: aktuelle Tabellen
	# .old_tables: Tabellen zum Programmstart
	#
	# Existierende aber nicht aus der DB geladene Tabellen sind NULL.
	def __init__(self, db=None):
		"""Initiiere ein Schema zu einer Datenbank."""
		self.db=db
		self.in_get=False

		self.old_tables={}
		self.tables={}
		global dbid
		dbid += 1
		self.db_id = dbid

	def __str__(self):
		return "db_%d" % (self.db_id,)
	
	def load_table(self,db,name):
		"""Lies eine Tabelle aus der DB"""

		if not db.db:
			return None
		if verbose:
			trace("Reading",db,name)
		x = db.db.DoFn("show create table `%s`" % (name,))
		n,desc=x[0:2]

		# print desc
		desc=desc.rstrip()
		if desc[-1] != ";": desc += ";"

		P = SQL(SQLScanner(desc))
		try: P.goal(self)
		except runtime.SyntaxError, e:
			runtime.print_error(e, P._scanner)
			sys.exit(1)

		t = self.tables[name]
		return t
	
	def get_table(self,name):
		"""Liefere eine Tabelle. Lies sie aus der DB, wenn möglich"""

		if self.in_get: return None

		# die Tabelle wird geladen, wenn sie noch nicht ablegegt ist
		try:
			t=self.tables[name]
		except KeyError:
			raise NoSuchTableError(name)
		else:
			if t is not None: return t

		try:
			self.in_get=True
			t = self.load_table(self,name)
		finally:
			self.in_get=False

		return t
	
	def scan(self,*a,**k):
		P = SQL(SQLScanner(*a,**k))
		try: P.goal(self)
		except runtime.SyntaxError, e:
			runtime.print_error(e, P._scanner)
			sys.exit(1)

	def dump(self, tables=(),skip_flags=""):
		if tables:
			tv=tables
		else:
			tv=self.tables.keys()
			tv.sort()
		tvv=[]

		for tn in tv:
			t=self.tables[tn]
			if t is None:
				#del self.old_tables[tn]
				continue
			try: ot=self.old_tables[t.name]
			except KeyError: yield t.dump(skip_flags=skip_flags)
			else:
				if ot is None:
					yield t.dump(skip_flags=skip_flags)
				else:
					r=t.diff1(ot,skip_flags=skip_flags)
					if r: yield r
					#del self.old_tables[t.name]
					tvv.append((t,ot))

		for t,ot in tvv:
			r,rb = t.diff2(ot,skip_flags=skip_flags)
			if r:
				yield r
				if verbose:
					print rb
		if "k" not in skip_flags:
			for t,ot in tvv:
				r=t.diff3(ot,skip_flags=skip_flags)
				if r: yield r

		otv=self.old_tables.keys()
		otv.sort()
		for tn in otv:
			# ot=old_tables[tn]
			if tn not in self.tables and (not tables or tn in tables):
				yield "DROP TABLE `%s`" % (tn,)


	def read_data(db,tables=()):
		if not tables:
			tables=self.tables.keys()
			tables.sort()
		for t in tables:
			trace("reading data from "+t.name)
			for d in db.DoSelect("select * from "+bq(t.name), _dict=1,_empty=1):
				t.update(d)

	def update2(self, days=None, force=False,force_equal=False, tables=()):
		"""Generate data update statements if at least one source is not a database"""
		# The problem here are incomplete update statements which skip
		# auto-increment keys and default values. Therefore we'll need to
		# read all data and find equivalent records using appropriate keys.

		if not tables:
			tables=self.tables.keys()
			tables.sort()

		for t in tables:
			t1 = self.tables[t]
			try:
				t2 = self.old_tables[t]
			except KeyError:
				t2 = None
			while True:
				try:
					k,v = t1.contents.popitem()
				except KeyError:
					break
				else:
					vv = t1.pop(v)
					assert vv is None or vv is v,"%r %r" % (v,vv)
					if t2 is None: w = None
					else: w = t2.get(v)
					if w is None:
						s,k = print_insert(v)
						yield (None,"insert into "+bq(t)+s,k)
						continue
					ww = t2.pop(w)
					assert ww is None or ww is w,"%r %r" % (w,ww)
					vxw = notcommon(v,w) # the values-to-be-written
					if not vxw: # nothing to do
						continue
					vw = common(v,w) # candidate fields for the key
					tk = t1.possible_key(vw) # actual key
	
					tk = tuple(( f[0].name for f in tk[0].fields ))
					sk,skl = print_query(common(vw,tk)) # where clause
					uk,ukl = print_update(vxw) # assignments
					skl.update(ukl) # common dictionary
					yield (None, "update "+bq(t)+" set "+uk+" where "+sk, skl)

			if t2: # now process the remaining stuff
				while True:
					try:
						k,v = t2.contents.popitem()
					except KeyError:
						break
					else:
						vv = t2.pop(v)
						assert vv is None or vv is v,"%r %r" % (v,vv)
						tk = t2.possible_key(v)
						tk = tuple(( f[0].name for f in tk[0].fields ))
						sk,skl = print_query(common(vw,tk)) # where clause
						yield (None, "delete from "+bq(t)+" where "+sk, skl)


	def update(self, db1,db2, days=None, force=False,force_equal=False, tables=()):
		"""Generate data update statements if both sources are databases"""


		odb=Db(db1)
		odbq=Db(db1)
		ndb=Db(db2)
		ndbq=Db(db2)


		def iseq(a,b):
			if not a and not b: return 1
			if not a or not b: return 0
			
			for aa,bb in zip(a,b):
				if not aa and not bb: continue
				if not aa or not bb: return 0
				if aa != bb: return 0

			return 1

		def cmp_index(xa,xb):
			if not xa: return 1
			if not xb: return -1

			if len(xa) != len(xb): raise RuntimeError("Indexproblem")
			for va,vb in zip(xa,xb):
				try:
					na=int(va)
					nb=int(vb)
				except ValueError:
					if va.lower() < vb.lower(): return -1
					if va.lower() > vb.lower(): return 1
				else:
					if na < nb: return -1
					if na > nb: return 1
			return 0


		def key(ts_field,ts_field_pos,ar,br,fieldpos,fld):
			res = ""
			resl={}
			for fd in fld:
				if res != "": res += " and "
				res += bq(fd)+"="
				try:
					val = ar[fieldpos[fd]]
				except TypeError: # try to figure out what's wrong
					print "fld",fld
					print "fd",fd
					print "fieldpos",fieldpos
					print "ar",ar
					raise

				if val is None:
					res += "NULL"
				else:
					if not isinstance(val,array.array):
						#try: val=int(val)
						#except OverflowError: val=float(val)
						#except ValueError: pass
						pass
					res += '${%s}' % (fd,)
					resl[fd]=val

			if br and ts_field:
				if res != "": res += " and "
				if br[ts_field_pos] is None:
					res += " "+ts_field+" is NULL"
				else:
					res += " "+ts_field+" = ${"+ts_field+"}"
					resl[ts_field] = br[ts_field_pos]

			return (res,resl)


		def next_row(dbg,table,query):
			try: res = query.next()
			except StopIteration: return None

			# tracing: print message once per second or when the table name changes
			global cnt
			global cntt
			global ltm

			ntm=time.time()
			if cntt != table:
				ltm=ntm-1
				cntt=table
			if ntm-1 >= ltm:
				trace(cnt,dbg,table.name,"|".join(map(str,res)))
				ltm=ntm
			cnt += 1

			return res


		def load(fl,dbg,table,db,f,fk):
			flen = len(f)

			if not fl: raise RuntimeError("NoFLerr")
			if f:
				if len(fl) != len(f): raise RuntimeError("BadFLerr",fl,f)
			else:
				fl=[]
			if not fk: return fl
			vals={}

			cmd=""
			for val,fn in zip(fl,f):
				if val is None:
					val = "is NULL"
				else:
					vals[fn] = val
					val = "= ${%s}" % (fn,)

				if cmd != "": cmd += " and "
				cmd += bq(fn)+" "+val

			fl = [ f_ for f_ in fl ]

			def tsn(s):
				#if s.tname == "timestamp": return '0+'+bq(s.name)
				return bq(s.name)

			try:
				if cmd:
					fl += db.DoFn("select "+",".join(map(tsn,fk))+" from "+table.name+" where "+cmd,**vals)
				else:
					fl += db.DoFn("select "+",".join(map(tsn,fk))+" from "+table.name)
			except NoData:
				return None
			assert len(fl) == flen+len(fk)
			return fl

		ov=[]
		def out(db,tx,**txf):
			if verbose>3:
				print "%s;" % (tx,)
				print "# %s" % (txf,)
				#if ctx:
				#	print "# %s" % (ctx,)
			return (db,tx,txf)

		if days:
			days = " >= FROM_UNIXTIME("+str(int(time.time()-24*3600*days))+")"
		else:
			days = ""

		# yield out(ndb, "SET SQL_LOG_BIN = 0")
		# yield out(ndb, "SET FOREIGN_KEY_CHECKS = 0")

		def do_tab(table):
			global cnt
			cnt=0

			keys=()
			fld=[]

			try:
				old_table=self.old_tables[table.name]
			except KeyError:
				print >>sys.stderr,"# Table %s does not exist" % (table.name)
				return
			for flk in table.best_keys():
				keys = [ f[0].name for f in flk.fields ]
				for f in keys:
					if not f in old_table.col:
						keys=()
						break
				if keys:
					break

			fieldpos={} # index to (index and other) fields
			i = 0
			for f in keys:
				fieldpos[f] = i
				i += 1

			for f in table.col.keys():
				if f not in old_table.col:
					continue
				if f in keys:
					continue
				f=table.col[f]
				fld.append(f)
				fieldpos[f.name] = i
				i += 1

			ts_field = table.timestamp()
			ts_sel = ""
			if ts_field:
				#ts_sel = "0+"+bq(ts_field.name)+","
				ts_sel = bq(ts_field.name)+","
				ts_field_pos = fieldpos[ts_field.name]
				ts_field = ts_field.name
			else:
				ts_field_pos = None

			trace(0,table.name,keys)
			if keys:
				tt = ""
				if days and ts_field is not None:
					tt = " where "+ts_field+days
				ost = odbq.DoSelect("select "+ts_sel+" "+",".join(map(bq,keys))+" from "+table.name+" "+tt+" order by "+",".join(map(bq,keys)), _store=0,_empty=1)
				nst = ndbq.DoSelect("select "+ts_sel+" "+",".join(map(bq,keys))+" from "+table.name+" "+tt+" order by "+",".join(map(bq,keys)), _store=0,_empty=1)
			else:
				ost = odbq.DoSelect("select "+ts_sel+"1 from "+table.name, _store=0,_empty=1)
				nst = ndbq.DoSelect("select "+ts_sel+"2 from "+table.name, _store=0,_empty=1)

			index_a = next_row("src",table,ost)
			index_b = next_row("dst",table,nst)

			while index_a or index_b:
				if ts_field and index_a:
					old_ts=index_a[0]
					dxa=index_a[1:]
				elif index_a:
					old_ts=0
					dxa=index_a
				else:
					dxa=None

				if ts_field and index_b:
					new_ts=index_b[0]
					dxb=index_b[1:]
				elif index_b:
					new_ts=1
					dxb=index_b
				else:
					dxb=None

				row_diff = cmp_index(dxa,dxb)

				# If two entries match, don't bother with the fetch.
				if not (row_diff==0 and not force_equal and ts_field is not None and old_ts==new_ts):
					if row_diff>0: dfa=dxb
					else: dfa=dxa
					if row_diff<0: dfb=dxa
					else: dfb=dxb
					xa = load(dfa, "src",table,odb,keys,fld)
					xb = load(dfb, "dst",table,ndb,keys,fld)

					if xa and xb: # both tables have data
						if ts_field:
							try:
								idc= (xa[ts_field_pos] > xb[ts_field_pos])
							except TypeError:
								idc= (xb[ts_field_pos] is None and xa[ts_field_pos] is not None)
						else:
							idc=0
						if row_diff or idc or not iseq(xa,xb):
							if force or idc:
								wk,wkl = key(ts_field,ts_field_pos,xa,xb,fieldpos,keys)
								pn,pnl = print_update(posval2dict(fieldpos,xa,xb))
								if pn:
									pnl.update(wkl)
									if keys:
										yield out(ndb,"update %s set %s \n\t where %s" % (bq(table.name),pn,wk), _empty=1, **pnl)
									else:
										yield out(ndb,"update %s set %s" % (bq(table.name),pn),**pnl)
					elif row_diff < 0: # alt => einfügen
						if xa:
							pn,pnl = print_insert(posval2dict(fieldpos,xa))
							if pn: yield out(ndb, "replace into %s %s" % (bq(table.name),pn),**pnl)
					elif xb: # neu => raus
						wk,wkl = key(ts_field,ts_field_pos,xb,None,fieldpos,keys)
						pn,pnl = print_update(posval2dict(fieldpos,xb))
						pnl.update(wkl)
						if keys:
							yield out(ndb, "delete from %s where %s" % (bq(table.name),wk),**pnl)
						else:
							yield out(ndb, "delete from %s" % (bq(table.name),),**pnl)
					else: # kein 'xb' : ???
						print >>sys.stderr, "XB leer",ts_field,ts_field_pos,fieldpos,keys
				if keys:
					if row_diff <= 0: index_a = next_row("src",table,ost)
					if row_diff >= 0: index_b = next_row("dst",table,nst)
				else:
					index_a=None
					index_b=None
					for st in (ost,nst):
						try: st.next()
						except StopIteration: pass
						try: st.next()
						except StopIteration: pass
						else: raise RuntimeError("TooManydata")

		def _trans():
			#if opts.execute or opts.execstr:
			#	ndb.Do("SET SQL_LOG_BIN = 0", _empty=1)
			#	ndbq.Do("SET SQL_LOG_BIN = 0", _empty=1)
			#if opts.execstr:
			#	ndb.Do("SET FOREIGN_KEY_CHECKS = 0", _empty=1)
			#	ndbq.Do("SET FOREIGN_KEY_CHECKS = 0", _empty=1)
			ndb.Do("SET UNIQUE_CHECKS = 0", _empty=1)
			ndbq.Do("SET UNIQUE_CHECKS = 0", _empty=1)

			odbq.Do("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", _empty=1)
			odbq.Do("START TRANSACTION WITH CONSISTENT SNAPSHOT", _empty=1)
			if verbose > 1:
				print "Master:",odbq.DoFn("SHOW MASTER STATUS", _dict=1)

			for table in args:
				for res in do_tab(self.get_table(table)):
					yield res

		for res in odbq.transact_iter(_trans):
			yield res

			odb.commit()
			ndb.commit()
			ndbq.commit()
		odbq.commit()


def main():
	global args,verbose,_debug

	try: from optparse import OptionParser
	except ImportError: from optik import OptionParser

	parser = OptionParser(conflict_handler="resolve")
	parser.add_option("-h","--help","-?", action="help",
						help="print this help text")
	parser.add_option("-v", "--verbose", dest="verbose", action="count",
						help="moer verbose progress reports", default=1)
	parser.add_option("-q", "--quiet", dest="verbose", action="store_const",
						help="no progress reports", const=0)
	parser.add_option("-s", "--source", dest="db1", default=None,
						help="database B, up-to-date tables")
	parser.add_option("-d", "--dest", dest="db2", default=None,
						help="database A, old tables")
	parser.add_option("-S", "--src", dest="db1file", default=None,
						help="data description B, up-to-date tables")
	parser.add_option("-D", "--dst", dest="db2file", default=None,
						help="data description A, old tables")
	parser.add_option("-c","--continue", action="store_true", dest="cont",
						help="continue after errors", default=False)
	parser.add_option("-i","--init", action="store_true", dest="init",
						help="CREATE statements in input", default=False)
	parser.add_option("-X","--exec-struct", action="store_true", dest="execstr",
						help="run structure-changing commands", default=False)
	parser.add_option("-u","--update", action="store_true", dest="update",
						help="compare data of A and B", default=False)
	parser.add_option("-x","--exec", action="store_true", dest="execute",
						help="run data updating commands", default=False)
	parser.add_option("-F","--force", action="store_true", dest="force",
						help="ignore all timestamps", default=False)
	parser.add_option("-f","--force-equal", action="store_true", dest="force_equal",
						help="ignore equal timestamps", default=False)
	parser.add_option("-t","--days", action="store", dest="days", type="int",
						help="only consider the last N days", default=0)
	parser.add_option("-p","--preload", action="store_true", dest="preload",
						help="pre-load data descriptions", default=False)
	parser.add_option("-n","--skip-change", action="store", dest="skip_flags",
						help="Skip: c=Charset,a=FieldOrder,k=ForeignKey,i=keyName,p=pack,m=max,e=engine", default="")
	parser.add_option("-N","--skip-table", action="store", dest="skip_tables",
						help="skip these tables", default="")

	# XXX TODO allow more than one input file

	(opts, args) = parser.parse_args()
	verbose=opts.verbose
	if verbose>1:
		_debug=sys.stderr

	if opts.db1 or opts.db2 or opts.update:
		global Db,NoData
		from sqlmix import Db,NoData

	if opts.db1file is not None and opts.db2file is not None and \
			opts.db1file == "-" and opts.db2file == "-":
		print >>sys.stderr,"You can't read both files from standard input"
		sys.exit(1)

	args2 = args
	skips = opts.skip_tables.split(",")

	# from A to B: so you update B: so the old data is in B.
	# Thus we read B first.

	db2=None
	if opts.db2file:
		if opts.db2:
			print >>sys.stderr,parser.format_help()
			sys.exit(1)
		trace("Processing",opts.db2file)
		if opts.db2file == "-":
			f=sys.stdin
		else:
			f=open(opts.db2file,"r")
		db2=Schema()
		try: db2.scan("", filename=opts.db2file, file=f)
		except runtime.SyntaxError, e:
			runtime.print_error(e, P._scanner)
			sys.exit(1)

		f.close()

	elif opts.db2:
		db2=Db(opts.db2)
		if opts.execute or opts.execstr:
			db2.Do("SET SQL_LOG_BIN = 0", _empty=1)
		if opts.execstr:
			db2.Do("SET FOREIGN_KEY_CHECKS = 0", _empty=1)

		db2=Schema(db2)

		if opts.preload:
			for t in db2.tables.keys():
				db2.get_table(t)

		if not args2:
			args2 = [ t for t, in db2.db.DoSelect("show tables", _store=1) if t not in skips ]
		for t in args2:
			try:
				db2.load_table(db2,t)
			except mc.Connection.ProgrammingError:
				pass

	else:
		print >>sys.stderr,parser.format_help()
		sys.exit(1)

	if opts.db1file:
		if opts.db1:
			print >>sys.stderr,parser.format_help()
			sys.exit(1)
		trace("Processing",opts.db1file)
		if opts.db1file == "-":
			f=sys.stdin
		else:
			f=open(opts.db1file,"r")
		db1=Schema(db2)

		if not opts.init:
			db1.tables={}
			for k,v in db2.tables.iteritems():
				db1.tables[k] = v.clone(db1)

		try: db1.scan("", filename=opts.db1file, file=f)
		except runtime.SyntaxError, e:
			runtime.print_error(e, P._scanner)
			sys.exit(1)

		f.close()

	elif opts.db1:
		db1=Db(opts.db1)
		if opts.execute or opts.execstr:
			db1.Do("SET SQL_LOG_BIN = 0", _empty=1)
		if opts.execstr:
			db1.Do("SET FOREIGN_KEY_CHECKS = 0", _empty=1)
		db1=Schema(db1)

		if not opts.init:
			db1.tables = {}
			for k,v in db2.tables.iteritems():
				db1.tables[k]=v.clone(db1)

		if not args:
			args = [ t for t, in db1.db.DoSelect("show tables", _store=1) if t not in skips ]

		for t in args:
			db1.load_table(db1,t)

		if opts.preload:
			for t in db2.tables.keys():
				db2.get_table(t)
	else:
		if args:
			print >>sys.stderr,parser.format_help()
			sys.exit(1)

	db1.old_tables=db2.tables

	exitcode = 0
	for t in db1.dump(tables=args,skip_flags=opts.skip_flags):
		if t is None: continue

		if not opts.execstr: t += ";"
		if verbose>2 or not opts.execstr:
			print_fkey_check(0)
			print t

		if opts.execstr:
			db2.db.Do(t, _empty=True)

		exitcode=2

	if opts.update:
		def exc_handle(x,*a,**k):
			try:
				x(*a,**tx)
			except KeyboardInterrupt:
				sys.exit(1)
			except:
				if opts.cont:
					print >>sys.stderr,format_exception_only(*(sys.exc_info()[0:2]))

				else:
					raise

		updl=[]
		updb = []
		ins=0
		upd=0
		rem=0
		com=0

		if not opts.db1file and not opts.db2file:
			# read from database
			updo = db1.update(opts.db1,opts.db2, days=opts.days, force=opts.force,force_equal=opts.force_equal, tables=args)
		else:
			# analyze actual table dumps
			if not opts.db1file: db1.read_data(tables=args)
			if not opts.db2file: db2.read_data(tables=args)
			updo = db1.update2(days=opts.days, force=opts.force,force_equal=opts.force_equal, tables=args)
			
		for d,t,tx in updo:
			if t is None: continue
			exitcode=2

			if opts.execute:
				if t.startswith("insert") or t.startswith("replace"):
					ins += 1
					exc_handle(d.Do,t,**tx)
				elif t.startswith("delete"):
					rem += 1
					updl.append((d,t,tx))
				else:
					upd += 1
					updl.append((d,t,tx))
				if d not in updb:
					updb.append(d)
			else:
				def _prep(name):
					return sqlquote(tx[name.group(1)])
				t = re.sub(r"\$\{([a-zA-Z][a-zA-Z_0-9]*)\}",_prep,t)
				print t,";"
		for d,t,tx in updl:
			if verbose:
				com += 1
				if not com % 100:
					trace("update",com)
			exc_handle(d.Do,t,**tx)
		trace("Commit")
		for d in updb: d.commit()
		if verbose>1:
			print "+%d -%d /%d  | " % (ins,rem,upd)

	print_fkey_check(1)
	return exitcode


### PARSER ###

%%
parser SQL:
	ignore: '[ \r\t\n]+'
	ignore: '#.*?\n'
	ignore: '--\\s.*?\n'
	ignore: '--\n'
	ignore: '/\\*.*?\\*/'
	# TODO: multi-line /*...*/ comments

	token END: "$"

	token ACTION: "(?i)ACTION"
	token ADD: "(?i)ADD"
	token AFTER: "(?i)AFTER"
	token ALTER: "(?i)ALTER"
	token AUTO_INCREMENT: "(?i)AUTO_INCREMENT"
	token CASCADE: "(?i)CASCADE"
	token CHANGE: "(?i)CHANGE"
	token CHARACTER: "(?i)CHARACTER"
	token CHARSET: "(?i)CHARSET"
	token COLUMN: "(?i)COLUMN"
	token COLLATE: "(?i)COLLATE"
	token CONSTRAINT: "(?i)CONSTRAINT"
	token CREATE: "(?i)CREATE"
	token CURRENT_TIMESTAMP: "(?i)CURRENT_TIMESTAMP"
	token DEFAULT: "(?i)DEFAULT"
	token DELETE: "(?i)DELETE"
	token DROP: "(?i)DROP"
	token ENUM: "(?i)ENUM"
	token ENGINE: "(?i)ENGINE"
	token EXISTS: "(?i)EXISTS"
	token FALSE: "(?i)FALSE"
	token FOREIGN: "(?i)FOREIGN"
	token FIRST: "(?i)FIRST"
	token GLOBAL: "(?i)GLOBAL"
	token INSERT: "(?i)INSERT"
	token INTO: "(?i)INTO"
	token IF: "(?i)IF"
	token KEY: "(?i)KEY"
	token LOCK: "(?i)LOCK"
	token MAX_ROWS: "(?i)MAX_ROWS"
	token MODIFY: "(?i)MODIFY"
	token NO: "(?i)NO"
	token NOT: "(?i)NOT"
	token NULL: "(?i)NULL"
	token ON: "(?i)ON"
	token PACK_KEYS: "(?i)PACK_KEYS"
	token PRIMARY: "(?i)PRIMARY"
	token READ: "(?i)READ"
	token REFERENCES: "(?i)REFERENCES"
	token RESTRICT: "(?i)RESTRICT"
	token SESSION: "(?i)SESSION"
	token SET: "(?i)SET"
	token TABLE: "(?i)TABLE"
	token TABLES: "(?i)TABLES"
	token TRUE: "(?i)TRUE"
	token TYPE: "(?i)TYPE"
	token UNIQUE: "(?i)UNIQUE"
	token UNLOCK: "(?i)UNLOCK"
	token UNSIGNED: "(?i)UNSIGNED"
	token UPDATE: "(?i)UPDATE"
	token VALUES: "(?i)VALUES"
	token WRITE: "(?i)WRITE"

	token NAME: "(?i)[a-z][a-z_0-9]*"
	token XNAME: "(?i)[^`]+"
	token SQTEXT: "[^']*"
	token NUM: "-?[0-9]+"

	rule goal<<db>>:
		{{ self.db = db }}
		( statement ? ";" )* END
	
	rule statement:
		s_create | s_alter | s_drop | s_insert | s_set | s_lock | s_unlock

	rule s_lock:
		LOCK TABLES qname ( READ | WRITE ) ?

	rule s_unlock:
		UNLOCK TABLES

	rule s_drop:
		DROP TABLE (
			IF EXISTS qname {{ if qname in self.db.tables: self.db.get_table(qname).drop() }}
			| qname {{ self.db.get_table(qname).drop() }}
			)

	rule s_alter_add<<t>>:
		ADD s_create_def<<t>>

	rule s_alter_change<<t>>:
		CHANGE COLUMN? qname s_create_col<<t,qname>>

	rule s_alter_modify<<t>>:
		MODIFY COLUMN? s_create_col<<t,True>>

	rule s_alter_del<<t>>:
		DROP
		( ( PRIMARY KEY? {{ name=None }} 
		       | UNIQUE KEY? qname {{ name=qname }}
			   | KEY qname {{ name=qname }}
			   ) 
		  {{ t.del_key(name) }}
		| ( CONSTRAINT | FOREIGN KEY? ) qname {{ t.del_fkey(qname) }}
		| COLUMN? qname {{ t.del_field(qname) }}
		)

	rule s_alter_decl<<t>>:
		( s_alter_add<<t>>
		| s_alter_del<<t>>
		| s_alter_change<<t>>
		| s_alter_modify<<t>>
		)* s_table_opt<<t>>*

	rule s_alter:
		ALTER TABLE qname {{ t=self.db.get_table(qname); trace(qname) }}
		s_alter_decl<<t>> ( "," s_alter_decl<<t>> )*

	rule s_create:
		CREATE TABLE qname {{ t=Table(qname,self.db); trace(qname) }}
		"\(" s_create_def<<t>> ( "," s_create_def<<t>> )*
		     s_create_decl<<t>>* "\)"
		s_table_opt<<t>>*

	rule s_table_opt<<t>>:
		( ENGINE | TYPE ) "=" qname {{ t.engine=qname }}
		| AUTO_INCREMENT "="  NUM
		| DEFAULT? ( CHARACTER SET | CHARSET ) "=" qname {{ t.charset=qname; t.collation=None; }}
		| COLLATE "=" qname {{ t.collation=qname }}
		| PACK_KEYS "=" val {{ t.packing=val }}
		| MAX_ROWS "=" val {{ t.maxrows=val }}

	rule s_create_foreign<<t,name>>:
		{{ fl1=[]; fl2=[] }}
		"\(" qname {{ fl1.append(t.col[qname]) }}
			( "," qname {{ fl1.append(t.col[qname]) }} )* "\)"
		REFERENCES rqname {{ rtable=qname }}
		"\(" qname {{ fl2.append(qname) }}
			( "," qname {{ fl2.append(qname) }} )* "\)"
		{{ opt={"update":"cascade","delete":"restrict"} }}
		( 
		  ON ( UPDATE ref_opt {{ opt["update"]=ref_opt.lower() }} 
		     | DELETE ref_opt {{ opt["delete"]=ref_opt.lower() }}
		) )*
		{{ t.new_fkey(name,fl1,rtable,fl2, opt) }}

	rule ref_opt:
		CASCADE {{ return "CASCADE" }}
		| DELETE {{ return "DELETE" }}
		| RESTRICT {{ return "RESTRICT" }}
		| SET NULL {{ return "SET NULL" }}
		| NO ACTION {{ return "NO ACTION" }}

	rule s_create_def<<t>>:
		( PRIMARY KEY? {{ ktyp="U"; qname=None }}
		| UNIQUE KEY? {{ ktyp="U"; qname="" }} qname?
		| KEY {{ ktyp="-"; qname="" }} qname?
		) {{ name=qname; pkey= [] }}
		"\(" qname {{ klen=None }} ( "\(" val {{ klen=val }} "\)" ) ?
				{{ pkey.append((t.col[qname],klen)); klen=None }} 
		  ( "," qname {{ klen=None }} ( "\(" val {{ klen=val }} "\)" ) ?
		  		{{ pkey.append((t.col[qname],klen)) }} )* "\)"
		  {{ t.new_key(name,ktyp,pkey) }}
		| ( ( CONSTRAINT {{ name=None }} ( qname {{ name=qname }} )? )
		  | {{ name=None }} )
		  FOREIGN KEY? ( rqname {{ if name is None: name=rqname }} )?
		  s_create_foreign<<t,name>>
		| s_create_col<<t,None>>

	rule s_col_enum<<c>>:
		{{ vals=[] }}
		"\(" val {{ vals.append(val) }} 
		( "," val {{ vals.append(val) }} )* "\)"
		{{ c.tname = tuple(vals) }}

	rule s_create_col<<t,oc>>:
		qname {{ c=t.new_field(qname,oc) }} 
			( ENUM s_col_enum<<c>>
			| qname {{ c.tname = qname.lower() }}
			  ( "\(" NUM {{ c.len=int(NUM) }}
			    ( "," NUM {{ c.len2 = int(NUM) }} ) ? "\)" ) ? 
			)
			(
			  NULL {{ c.nullable=True }}
			| PRIMARY KEY {{ t.new_key(None,"U",[(c,None)]) }}
			| NOT NULL {{ c.nullable=False }}
			| AUTO_INCREMENT {{ c.autoinc=True }}
			| UNSIGNED {{ c.unsigned=True }}
			| (CHARSET | CHARACTER SET) qname {{ c.charset=qname }}
			| COLLATE qname {{ c.collation=qname }}
			| DEFAULT ntval {{ c.defval=ntval }} 
			| ON UPDATE ntval {{ c.updval=ntval }} 
			| FIRST {{ c.set_after(None,oc) }}
			| AFTER qname {{ c.set_after(t.col[qname],oc) }}
			)* {{ c.create_done() }}

	rule s_create_decl<<t>>:
		TYPE "=" NAME
		
	rule s_insert<<t>>: {{ cols=[]; vals=[] }}
			INSERT INTO qname {{ t=self.db.get_table(qname) }}
			( SET qname "=" nval {{ cols.append(qname); vals.append(nval) }}
			   ( "," qname "=" nval {{ cols.append(qname); vals.append(nval) }} )*
			| ( "\(" qname {{ cols.append(qname) }}
			    ( "," qname {{ cols.append(qname) }} )* "\)" 
			  | {{ cols=t.colnames() }} )
			  VALUES "\(" nval {{ vals.append(nval) }}
			    ( "," nval {{ vals.append(nval) }} )* "\)"
			  ( "," {{ t.update(dict(zip(cols,vals))); vals = [] }} "\("
			      nval {{ vals.append(nval) }} 
			    ( "," nval {{ vals.append(nval) }} )* "\)" )*
			  ) {{ t.update(dict(zip(cols,vals))) }}

	rule s_set: SET ( SESSION | GLOBAL )? qname "=" val

	rule qname: "`" XNAME "`" {{ return XNAME.lower() }}
			| NAME {{ return NAME.lower() }}

	rule rqname: qname {{ n = qname }}
			( "\." qname {{ n += '.'+qname }} )? 
			{{ return n }}

	rule nval: NULL {{ return None }}
			| val {{ return val }}

	rule tval: TRUE {{ return '1' }}
			| FALSE {{ return '0' }}

	rule ntval: NULL {{ return None }}
			| CURRENT_TIMESTAMP {{ return curtime }}
			| sval {{ return sval }}
			| tval {{ return tval }}

	rule val: NUM {{ return int(NUM) }}
			| "'" SQTEXT "'" {{ return SQTEXT }}

	rule sval: NUM {{ return NUM }}
			| "'" SQTEXT "'" {{ return SQTEXT }}

%%

if __name__ == "__main__":
	sys.exit(main())
