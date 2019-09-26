# Copyright (c) 2017, Arthur Goncharuk
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import time
import struct
import marshal
import warnings

from os import urandom
from binascii import hexlify, unhexlify

from bsddb3.db import *
from . import register_close_handler

__all__ = [
    'DbTokens',
    'TokenError',
    'TokenInvalidError',
    'TokenExpiredError',
    'TokenNotFoundError',
    'urandompool'
]

class TokenError(Exception): pass
class TokenInvalidError(TokenError): pass
class TokenExpiredError(TokenError): pass
class TokenNotFoundError(TokenError, KeyError): pass


def urandompool(length, pool):
    cachesize = length * pool
    cache = urandom(cachesize)
    index = 0

    while True:
        if index == cachesize:
            cache = urandom(cachesize)
            index = 0
        
        yield cache[index:index+length]
        index += length


class DbTokens:
    def __init__(self,
                 dbdir,
                 keylen=24,
                 cachesize=(1024*1024)*32,
                 pagesize=8192,
                 maxgrouptokens=50,
                 refreshtime=2,
                 dumps=marshal.dumps,
                 loads=marshal.loads):

        self._dumps = dumps
        self._loads = loads

        self.dbenv = DBEnv()
        self.dbenv.set_cachesize(0, cachesize, 1)
        self.dbenv.open(dbdir,
                        DB_CREATE |
                        DB_REGISTER |
                        DB_RECOVER |
                        DB_INIT_TXN |
                        DB_INIT_MPOOL |
                        DB_INIT_LOCK)

        register_close_handler(self.dbenv.close)
        
        self.db_tokens = DB(self.dbenv)
        self.db_tokens.set_pagesize(pagesize)
        self.db_tokens.open('tokens.db', DB_RECNO, DB_CREATE, 0)
        register_close_handler(self.db_tokens.close)
        
        # Registry of all group tokens. group -> tokens ids
        self.db_groups = DB(self.dbenv)
        self.db_groups.set_flags(DB_DUP)
        self.db_groups.set_pagesize(pagesize)
        self.db_groups.open('groups.db', DB_BTREE, DB_CREATE, 0)

        register_close_handler(self.db_tokens.close)
        
        # Header of metadata: key, timestamp, ttl
        metastruct = struct.Struct('255p d d')
        self._metapack = metastruct.pack
        self._metaunpack = metastruct.unpack
        self._metalen = metastruct.size
        register_close_handler(self.db_groups.close)
        
        self._keypack = struct.Struct('255p').pack
        
        self.set_keylen(keylen)
        self.set_refreshtime(refreshtime)
        self.set_maxgrouptokens(maxgrouptokens)

    def __del__(self):
        self.close()

    def create(self, group, data, ttl):
        '''Creates token.
        group:
            A serializable python object that identifies the creator or user of the token. For example, it can be ID, login or network address for anonymous sources. For DOS prevention, if the number of tokens for group reaches the maximum (sets by DbTokens.set_maxgrouptokens), him oldest token will be deleted.
        data:
            Additional data object associated with the token.
        ttl:
            Token lifetime.

        Returns tuple of token and data:
            group
            data
            ttl
            timestamp
        '''
        key = next(self._keypool)
        timestamp = time.time()
        metadata = self._metapack(key, timestamp, ttl)
        payload = self._dumps((group, data))
        cursor = self.db_groups.cursor()

        try:
            tid = self.db_tokens.append(metadata+payload)

            if not tid:
                raise ValueError('Token insertion failed')

            cursor.put(self._dumps(group), str(tid).encode(), DB_KEYFIRST)

            # If the maximum is reached, the last token id will be deleted
            if cursor.count() > self._maxgrouptokens:
                last_tid = int(cursor.get(DB_LAST)[1])
                cursor.delete()
                
                try:
                    self.db_tokens.delete(last_tid)
                except DBKeyEmptyError: pass
        finally:
            cursor.close()

        return hexlify(self._tokenpack(tid, key)).decode(), {
            'data': data,
            'group': group,
            'ttl': ttl,
            'timestamp': timestamp
        }

    def get(self, token, rawdata=None):
        '''Tries to get token data "as is", including all metadata without checks.
        Returns tid, tkey, ((key, timestamp, ttl), (group, data))
        Exceptions:
            TokenInvalidError
        '''
        try:
            tid, tkey = self._tokenunpack(unhexlify(token))
            value = rawdata or self.db_tokens[tid]
            
            return (tid, tkey, (self._metaunpack(value[:self._metalen]),
                                self._loads(value[self._metalen:])))
        except Exception:
            raise TokenInvalidError(token)

    def authenticate(self, token):
        '''Perform token check.
        If successful, returns dict of token data:
            data
            group
            ttl
            timestamp
        Exceptions:
            TokenInvalidError, TokenExpiredError.
        '''
        tid, tkey, ((key, timestamp, ttl), (group, data)) = self.get(token)
        
        if tkey != key:
            raise TokenInvalidError(token)

        timecurrent = time.time()
        
        if timecurrent > timestamp + ttl:
            raise TokenExpiredError(token)

        # Update timestamp
        if timecurrent - timestamp > self._refreshtime:
            cursor = self.db_tokens.cursor()

            try:
                if cursor.set(tid, flags=DB_RMW, dlen=0, doff=0):
                    metadata = self._metapack(key, timecurrent, ttl)
                    cursor.put(0, metadata, flags=DB_CURRENT, dlen=self._metalen, doff=0)
            finally:
                cursor.close()

        return {
            'data': data,
            'group': group,
            'ttl': ttl,
            'timestamp': timestamp
        }

    def putdata(self, token, newdata):
        '''Rewrite token data.
        '''
        try:
            tid, tkey = self._tokenunpack(unhexlify(token))
        except Exception:
            raise TokenInvalidError(token)

        cursor = self.db_tokens.cursor()

        try:
            record = cursor.set(tid, flags=DB_RMW)
            
            if not record:
                raise TokenNotFoundError(token)
            else:
                rawdata = record[1]
            
            metadata = rawdata[:self._metalen]
            group, data = self._loads(rawdata[self._metalen:])
                                            
            payload = self._dumps((group, newdata))
            cursor.put(0, metadata+payload, flags=DB_CURRENT)
        finally:
            cursor.close()
        

    def rekey(self, token):
        '''Rewrite token key.
        '''
        
        try:
            tid, tkey = self._tokenunpack(unhexlify(token))
        except Exception:
            raise TokenInvalidError(token)

        cursor = self.db_tokens.cursor()

        try:
            if not cursor.set(tid, flags=DB_RMW, dlen=0, doff=0):
                raise TokenNotFoundError(token)

            key = next(self._keypool)
            cursor.put(0, self._keypack(key), flags=DB_CURRENT, dlen=255, doff=0)
        finally:
            cursor.close()
            
        return hexlify(self._tokenpack(tid, key))
    
    def remove(self, token):
        '''Removing token.
        '''
        tid, tkey, ((key, timestamp, ttl), (group, data)) = self.get(token)

        if tkey != key:
            raise TokenInvalidError(token)

        cursor = self.db_groups.cursor()

        try:
            self.db_tokens.delete(tid)
            cursor.set_both(self._dumps(group), str(tid).encode())
            cursor.delete()
        except (KeyError, DBInvalidArgError):
            pass
        finally:
            cursor.close()

    def removegroup(self, group):
        '''Remove all groups's tokens.
        '''
        group = self._dumps(group)
        cursor = self.db_groups.cursor()

        try:
            record = cursor.set(group)

            while record:
                try: self.db_tokens.delete(int(record[1]))
                except KeyError: pass
                
                record = cursor.next(DB_NEXT_DUP)

            self.db_groups.delete(group)
        except KeyError:
            pass
        finally:
            cursor.close()

    def set_keylen(self, keylen, pool=500):
        '''Sets token key length.
        All previous tokens of a different length will be invalid.
        '''
        if keylen > 255:
            raise ValueError('keylen must be <= 255')
        
        # tid, tkey
        tokenstruct = struct.Struct('I %is' % keylen)
        self._tokenpack = tokenstruct.pack
        self._tokenunpack = tokenstruct.unpack
        self._tokenlen = tokenstruct.size
        
        self._keypool = urandompool(keylen, pool)

    def set_refreshtime(self, timeout):
        '''Sets the timestamp resolution.
        It's reasonable to set the value about few seconds.
        '''
        self._refreshtime = timeout

    def set_maxgrouptokens(self, tokens):
        '''Sets maximum group tokens count.
        If the maximum is reached, the group's last token will be deleted.
        '''
        self._maxgrouptokens = tokens

    def sync(self):
        '''Flush cached pages to disk. May be called periodically.
        '''
        self.db_tokens.sync()
        self.db_groups.sync()

    def close(self):
        '''Closes the database of tokens.
        Important: this method should be called ALWAYS before the process is terminating, otherwise some of the cached data may not be saved.
        '''
        self.dbenv.close()



