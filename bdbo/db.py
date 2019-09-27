import types

from bsddb3.db import *

# Rename native interfaces
from bsddb3.db import DB as cDB
from bsddb3.db import DBEnv as cDBEnv
from bsddb3.db import DBSequence as cDBSequence

from .util import lexpacker
from . import register_close_handler

__all__ = [
    'DbEnv',
    'Db',
    'DbSequence'
    
] + [k for k in globals().keys() if k.startswith('DB_')]


class DbEnv(object):
    def __init__(self, *args, **kwargs):
        self._cobj = cDBEnv(*args, **kwargs)
        register_close_handler(self._cobj.close)

    def close(self, *args, **kwargs):
        return self._cobj.close(*args, **kwargs)
    
    def open(self, *args, **kwargs):
        return self._cobj.open(*args, **kwargs)

    def remove(self, *args, **kwargs):
        return self._cobj.remove(*args, **kwargs)

    def set_shm_key(self, *args, **kwargs):
        return self._cobj.set_shm_key(*args, **kwargs)

    def set_cachesize(self, *args, **kwargs):
        return self._cobj.set_cachesize(*args, **kwargs)

    def set_data_dir(self, *args, **kwargs):
        return self._cobj.set_data_dir(*args, **kwargs)

    def set_flags(self, *args, **kwargs):
        return self._cobj.set_flags(*args, **kwargs)

    def set_lg_bsize(self, *args, **kwargs):
        return self._cobj.set_lg_bsize(*args, **kwargs)

    def set_lg_dir(self, *args, **kwargs):
        return self._cobj.set_lg_dir(*args, **kwargs)

    def set_lg_max(self, *args, **kwargs):
        return self._cobj.set_lg_max(*args, **kwargs)

    def set_lk_detect(self, *args, **kwargs):
        return self._cobj.set_lk_detect(*args, **kwargs)

    def set_lk_max_locks(self, *args, **kwargs):
        return self._cobj.set_lk_max_locks(*args, **kwargs)

    def set_lk_max_lockers(self, *args, **kwargs):
        return self._cobj.set_lk_max_lockers(*args, **kwargs)

    def set_lk_max_objects(self, *args, **kwargs):
        return self._cobj.set_lk_max_objects(*args, **kwargs)

    def set_mp_mmapsize(self, *args, **kwargs):
        return self._cobj.set_mp_mmapsize(*args, **kwargs)

    def set_timeout(self, *args, **kwargs):
        return self._cobj.set_timeout(*args, **kwargs)

    def set_tmp_dir(self, *args, **kwargs):
        return self._cobj.set_tmp_dir(*args, **kwargs)

    def txn_begin(self, *args, **kwargs):
        return self._cobj.txn_begin(*args, **kwargs)

    def txn_checkpoint(self, *args, **kwargs):
        return self._cobj.txn_checkpoint(*args, **kwargs)

    def txn_stat(self, *args, **kwargs):
        return self._cobj.txn_stat(*args, **kwargs)

    def set_tx_max(self, *args, **kwargs):
        return self._cobj.set_tx_max(*args, **kwargs)

    def set_tx_timestamp(self, *args, **kwargs):
        return self._cobj.set_tx_timestamp(*args, **kwargs)

    def lock_detect(self, *args, **kwargs):
        return self._cobj.lock_detect(*args, **kwargs)

    def lock_get(self, *args, **kwargs):
        return self._cobj.lock_get(*args, **kwargs)

    def lock_id(self, *args, **kwargs):
        return self._cobj.lock_id(*args, **kwargs)

    def lock_put(self, *args, **kwargs):
        return self._cobj.lock_put(*args, **kwargs)

    def lock_stat(self, *args, **kwargs):
        return self._cobj.lock_stat(*args, **kwargs)

    def log_archive(self, *args, **kwargs):
        return self._cobj.log_archive(*args, **kwargs)

    def log_stat(self, *args, **kwargs):
        return self._cobj.log_stat(*args, **kwargs)

    def dbremove(self, *args, **kwargs):
        return self._cobj.dbremove(*args, **kwargs)

    def dbrename(self, *args, **kwargs):
        return self._cobj.dbrename(*args, **kwargs)

    def set_encrypt(self, *args, **kwargs):
        return self._cobj.set_encrypt(*args, **kwargs)

    def fileid_reset(self, *args, **kwargs):
        return self._cobj.fileid_reset(*args, **kwargs)

    def lsn_reset(self, *args, **kwargs):
        return self._cobj.lsn_reset(*args, **kwargs)


class Db(object):
    static_list = []
    
    def __init__(self, dbenv=None, flags=0):
        import marshal

        if isinstance(dbenv, DbEnv):
            self._cobj = cDB(dbenv._cobj, flags)
        else:
            self._cobj = cDB(dbenv, flags)

        register_close_handler(self._cobj.close)
        
        self.dbenv = dbenv
        self.keydump, self.keyload = lexpacker()
        self.datadump, self.dataload = marshal.dumps, marshal.loads
        self.capsule = lambda x: x

        self.rangecursor = types.MethodType(DbRangeCursor, self)

    def open(self, filename, dbname=None, dbtype=DB_UNKNOWN, flags=0, mode=0o660, txn=None):
        self._cobj.open(filename, dbname, dbtype, flags, mode, txn)

        if dbtype in (DB_RECNO, DB_QUEUE):
            self.keydump = int

    def encapsulate(self, class_or_callable):
        self.capsule = class_or_callable
        return self.capsule

    def append(self, *args, **kwargs):
        return self._cobj.append(*args, **kwargs)
    
    def associate(self, secdb, flags=0, txn=None):
        def decorator(callback):
            def wrapper(rkey, rdata):
                skeys = []

                for k in callback(self.keyload(rkey), self.dataload(rdata)):
                    skeys.append(self.keydump(k))

                return skeys
            
            self._cobj.associate(secdb._cobj, wrapper, flags, txn)

        return decorator
    
    def get(self, key, default=None, txn=None, flags=0, dlen=-1, doff=-1):
        rkey = self.keydump(key)
        rdata = self._cobj.get(rkey, default, txn, flags, dlen, doff)
        return self.capsule(self.dataload(rdata)) if rdata else None

    def put(self, key, data, txn=None, flags=0, dlen=-1, doff=-1):
        rkey = self.keydump(key)
        rdata = self.datadump(data)
        return self._cobj.put(rkey, rdata, txn, flags, dlen, doff)
    
    def delete(self, key, txn=None, flags=0):
        rkey = self.keydump(key)
        
        try:
            self._cobj.delete(rkey, txn, flags)
            return True
        except DBNotFoundError:
            return False

    def exists(self, key, txn=None, flags=0):
        rkey = self.keydump(key)
        return self._cobj.exists(rkey, txn, flags)
    
    def cursor(self, txn=None, flags=0):
        return self._cobj.cursor(txn, flags)
    
    def key_range(self, *args, **kwargs):
        return self._cobj.key_range(*args, **kwargs)
    
    def pget(self, *args, **kwargs):
        return self._cobj.pget(*args, **kwargs)

    def join(self, *args, **kwargs):
        return self._cobj.join(*args, **kwargs)

    def truncate(self, txn=None, flags=0):
        return self._cobj.truncate(txn, flags)

    def get_transactional(self):
        return self._cobj.get_transactional()
    
    def close(self, *args, **kwargs):
        return self._cobj.close(*args, **kwargs)
    
    def consume(self, *args, **kwargs):
        return self._cobj.consume(*args, **kwargs)

    def consume_wait(self, *args, **kwargs):
        return self._cobj.consume_wait(*args, **kwargs)

    def fd(self, *args, **kwargs):
        return self._cobj.fd(*args, **kwargs)

    def get_both(self, *args, **kwargs):
        return self._cobj.get_both(*args, **kwargs)

    def get_byteswapped(self, *args, **kwargs):
        return self._cobj.get_byteswapped(*args, **kwargs)

    def get_size(self, *args, **kwargs):
        return self._cobj.get_size(*args, **kwargs)

    def get_type(self, *args, **kwargs):
        return self._cobj.get_type(*args, **kwargs)

    def remove(self, *args, **kwargs):
        return self._cobj.remove(*args, **kwargs)

    def rename(self, *args, **kwargs):
        return self._cobj.rename(*args, **kwargs)

    def set_bt_minkey(self, *args, **kwargs):
        return self._cobj.set_bt_minkey(*args, **kwargs)

    def set_bt_compare(self, *args, **kwargs):
        return self._cobj.set_bt_compare(*args, **kwargs)

    def set_cachesize(self, *args, **kwargs):
        return self._cobj.set_cachesize(*args, **kwargs)

    def set_dup_compare(self, *args, **kwargs) :
        return self._cobj.set_dup_compare(*args, **kwargs)

    def set_flags(self, *args, **kwargs):
        return self._cobj.set_flags(*args, **kwargs)

    def set_h_ffactor(self, *args, **kwargs):
        return self._cobj.set_h_ffactor(*args, **kwargs)

    def set_h_nelem(self, *args, **kwargs):
        return self._cobj.set_h_nelem(*args, **kwargs)

    def set_lorder(self, *args, **kwargs):
        return self._cobj.set_lorder(*args, **kwargs)

    def set_pagesize(self, *args, **kwargs):
        return self._cobj.set_pagesize(*args, **kwargs)

    def set_re_delim(self, *args, **kwargs):
        return self._cobj.set_re_delim(*args, **kwargs)

    def set_re_len(self, *args, **kwargs):
        return self._cobj.set_re_len(*args, **kwargs)

    def set_re_pad(self, *args, **kwargs):
        return self._cobj.set_re_pad(*args, **kwargs)

    def set_re_source(self, *args, **kwargs):
        return self._cobj.set_re_source(*args, **kwargs)

    def set_q_extentsize(self, *args, **kwargs):
        return self._cobj.set_q_extentsize(*args, **kwargs)

    def stat(self, *args, **kwargs):
        return self._cobj.stat(*args, **kwargs)

    def sync(self, *args, **kwargs):
        return self._cobj.sync(*args, **kwargs)

    def type(self, *args, **kwargs):
        return self._cobj.type(*args, **kwargs)

    def upgrade(self, *args, **kwargs):
        return self._cobj.upgrade(*args, **kwargs)

    def verify(self, *args, **kwargs):
        return self._cobj.verify(*args, **kwargs)

    def set_get_returns_none(self, *args, **kwargs):
        return self._cobj.set_get_returns_none(*args, **kwargs)

    def set_encrypt(self, *args, **kwargs):
        return self._cobj.set_encrypt(*args, **kwargs)

    def get_flags(self, *args, **kwargs):
        return self._cobj.get_flags(*args, **kwargs)


# Range-based cursor for DB_BTREE databases
class DbRangeCursor:
    __slots__ = ['db', '_cursor', '_begin', '_end']

    def __init__(self, db, begin, end=None, txn=None, flags=0):
        self.db = db
        self._cursor = db._cobj.cursor(txn, flags | DB_CURSOR_BULK)
        self.set(begin, end)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, tb):
        self._cursor.close()

    def close(self):
        self._cursor.close()

    def set(self, begin, end=None):
        begin = self.db.keydump(begin)
        
        if end is None:
            end = begin[:-1] + b'~'
        else:
            end = self.db.keydump(end)

            if begin > end:
                end = end[:-1] + b'~'

        self._begin = begin
        self._end = end
        self._cursor.set_range(begin, 0, 0, 0)
    
    def first(self):
        self._cursor.set_range(self._begin, 0, 0, 0)
        return self

    def current(self):
        try:
            record = self._cursor.current()
        except DBInvalidArgError:
            return

        if record[0] >= self._begin and record[0] <= self._end:
            return self.db.capsule(self.db.dataload(record[1]))
    
    def offset(self, offset):
        try:
            self._cursor.set_recno(self._cursor.get_recno() + offset, 0, 0, 0)
        except DBInvalidArgError:
            pass
        
        return self

    def total(self):
        record = self._cursor.set_range(self._begin, 0, 0, 0)

        if not record:
            return 0

        begin_recno = self._cursor.get_recno()
        record = self._cursor.set_range(self._end, 0, 0, 0)
        
        if record:
            return self._cursor.get_recno() - begin_recno + (record[0] <= self._end)
        else:
            self._cursor.last(0, 0, 0)
            return self._cursor.get_recno() - begin_recno + 1

    def fetch(self, count):
        try:
            record = self._cursor.current()
        except DBInvalidArgError:
            return
        
        for i in range(count):
            if record and record[0] >= self._begin and record[0] <= self._end: 
                yield self.db.capsule(self.db.dataload(record[1]))
                record = self._cursor.next()
            else:
                break


class DbSequence:
    def __init__(self, db):
        if isinstance(db, Db):
            db = db._cobj
        
        self._cobj = cDBSequence(db)

    def close(self, *args, **kwargs):
        return self._cobj.close(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self._cobj.get(*args, **kwargs)

    def get_dbp(self, *args, **kwargs):
        return self._cobj.get_dbp(*args, **kwargs)

    def get_key(self, *args, **kwargs):
        return self._cobj.get_key(*args, **kwargs)

    def initial_value(self, *args, **kwargs):
        return self._cobj.initial_value(*args, **kwargs)

    def open(self, *args, **kwargs):
        return self._cobj.open(*args, **kwargs)

    def remove(self, *args, **kwargs):
        return self._cobj.remove(*args, **kwargs)

    def stat(self, *args, **kwargs):
        return self._cobj.stat(*args, **kwargs)

    def set_cachesize(self, *args, **kwargs):
        return self._cobj.set_cachesize(*args, **kwargs)

    def set_flags(self, *args, **kwargs):
        return self._cobj.set_flags(*args, **kwargs)

    def set_range(self, *args, **kwargs):
        return self._cobj.set_range(*args, **kwargs)

    def get_cachesize(self, *args, **kwargs):
        return self._cobj.get_cachesize(*args, **kwargs)

    def get_flags(self, *args, **kwargs):
        return self._cobj.get_flags(*args, **kwargs)

    def get_range(self, *args, **kwargs):
        return self._cobj.get_range(*args, **kwargs)


