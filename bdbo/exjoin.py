# Extended equality join

from types import MethodType
from itertools import product
from bsddb3.db import *


def eqjkeys(keys, **sorts):
    return product(keys, sorts.items())


class DbExJoinMixin:
    exjoin_db = None
    
    def __init__(self, dbenv=None, flags=0):
        super().__init__(dbenv, flags)
        self.exjoincursor = MethodType(DbExJoinCursor, self)
    
    def put(self, key, data, txn=None, flags=0, dlen=-1, doff=-1):
        txn = self.dbenv.txn_begin(txn)

        try:
            if self.exjoin_db is not None:
                self.exjoin_put(key, data, txn)
            
            ret = super().put(key, data, txn, flags, dlen, doff)
        except:
            txn.abort()
            raise
        else:
            txn.commit()
            return ret
    
    def delete(self, key, txn=None, flags=0):
        txn = self.dbenv.txn_begin(txn)

        try:
            if self.exjoin_db is not None:
                self.exjoin_delete(key, txn)

            ret = super().delete(key, txn, flags)
        except:
            txn.abort()
            raise
        else:
            txn.commit()
            return ret
    
    def cursor(self, txn=None, flags=0):
        raise NotImplementedError()
    
    def exjoin_put(self, key, data, txn):
        assert txn, 'Transaction must be specified'

        db = self.exjoin_db._cobj
        cursor = db.cursor(txn)
        
        try:
            rkey = self.keydump(key)
            old_data = self.get(key, txn=txn, flags=DB_RMW)

            if old_data:
                for k, (group, sort) in self.exjoin_keys_callback(key, old_data):
                    skey = self.keydump(group) + self.keydump(k)
                    sval = self.keydump(sort) + b'@' + rkey

                    if cursor.set_both(skey, sval):
                        cursor.delete()

            cursor.close()
            
            for k, (group, sort) in self.exjoin_keys_callback(key, data):
                skey = self.keydump(group) + self.keydump(k)
                sval = self.keydump(sort) + b'@' + rkey
                db.put(skey, sval, txn=txn, flags=DB_OVERWRITE_DUP)
                
        finally:
            cursor.close()

    def exjoin_delete(self, key, txn):
        assert txn, 'Transaction must be specified'
        
        cursor = self.exjoin_db._cobj.cursor(txn)

        try:
            rkey = self.keydump(key)
            old_data = self.get(key, txn=txn, flags=DB_RMW)

            if old_data:
                for k, (group, sort) in self.exjoin_keys_callback(key, old_data):
                    skey = self.keydump(group) + self.keydump(k)
                    sval = self.keydump(sort) + b'@' + rkey
                
                    if cursor.set_both(skey, sval):
                        cursor.delete()
        finally:
            cursor.close()

    @staticmethod
    def exjoin_keys_callback(key, data):
        'By default'
        return []
    
    def exjoin_associate(self, dbs, flags=0, txn=None):
        if self.get_flags() & (DB_DUP | DB_DUPSORT):
            msg = 'Primary databases may not be configured with duplicates'
            raise RuntimeError(msg)
        
        if not (self.get_transactional() and dbs.get_transactional()):
            msg = 'Databases with DbExJoinMixin must be transactional'
            raise RuntimeError(msg)
        
        def decorator(callback):
            self.exjoin_db = dbs
            self.exjoin_keys_callback = callback

            cursor = dbs._cobj.cursor(txn=txn)

            try:
                empty = not cursor.first()
            finally:
                cursor.close()

            if empty and (flags & DB_CREATE):
                self.exjoin_create(dbs, txn)

            return callback

        return decorator

    def exjoin_create(self, dbs, txn=None):
        cursor = self._cobj.cursor(txn, DB_CURSOR_BULK)
        
        try:
            db = dbs._cobj
            record = cursor.first()

            while record:
                rkey = record[0]
                key = self.keyload(rkey)
                data = self.dataload(record[1])
                
                for k, (group, sort) in self.exjoin_keys_callback(key, data):
                    skey = self.keydump(group) + self.keydump(k)
                    sval = self.keydump(sort) + b'@' + rkey
                    db.put(skey, sval, txn=txn, flags=DB_OVERWRITE_DUP)
                
                record = cursor.next()
        finally:
            cursor.close()


class DbExJoinCursor:
    __slots__ = ['db', 'counted', '_cursors', '_jcursor']

    def __init__(self, db, keys, group, txn=None):
        self.db = db
        self.counted = 0
        self._cursors = []
        self._jcursor = None

        if db.exjoin_db is None:
            return

        for key in keys:
            rkey = db.keydump(group) + db.keydump(key)
            cursor = db.exjoin_db._cobj.cursor(txn, DB_READ_COMMITTED)

            if cursor.set(rkey, dlen=0, doff=0):
                self._cursors.append(cursor)
            else:
                self.close()
                break

        if self._cursors:
            self._jcursor = db.exjoin_db._cobj.join(self._cursors)
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def close(self):
        if self._jcursor:
            self._jcursor.close()

        for c in self._cursors:
            c.close()

        self._cursors = []
        self._jcursor = None

    def skip(self, count):
        if not self._jcursor:
            return self
        
        i = 0
        key = None
        
        for i in range(count):
            key = self._jcursor.join_item()

            if not key:
                break

        self.counted += i + bool(key)
        return self

    def fetch(self, count, txn=None):
        if not self._jcursor:
            return
        
        i = 0
        key = None
        
        for i in range(count):
            key = self._jcursor.join_item()
            
            if key:
                pkey = key.rsplit(b'@', 1)[1]
                data = self.db._cobj.get(pkey, txn=txn)

                if data:
                    yield self.db.capsule(self.db.dataload(data))
                else:
                    # TODO: handle this rare and bad case
                    print('not found', pkey)
            else:
                break

        self.counted += i + bool(key)

