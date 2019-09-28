from hashlib import sha1

try:
    unicode
except NameError:
    unicode = str

literal_types = frozenset([
    bool,
    int,
    float,
    str,
    bytes,
    unicode,
    type(None)
])


def function_digest(f):
    code = f.__code__
    return sha1(marshal.dumps([
        code.co_argcount,
        code.co_nlocals,
        code.co_stacksize,
        code.co_flags,
        code.co_code,
        #code.co_consts,
        [x for x in code.co_consts if type(x) in literal_types],
        code.co_names,
        code.co_name,
        code.co_lnotab,
        code.co_freevars,
        code.co_cellvars
    ])).digest()


def lexpacker(tag_str=b'K', tag_bytes=b'P', tag_nint=b'U', tag_pint=b'V'):
    from struct import Struct
    from binascii import hexlify, unhexlify
    
    dump64 = Struct('>q').pack
    load64 = Struct('>q').unpack
    
    def lexdump(key, result=None):
        result = result if result is not None else []
        
        if not isinstance(key, list):
            key = [key]

        for k in key:
            if isinstance(k, str):
                result.append(tag_str + hexlify(bytes(k, 'utf8')))

            elif isinstance(k, int):
                if k < 0:
                    result.append(tag_nint + hexlify(dump64(k)))
                else:
                    result.append(tag_pint + hexlify(dump64(k)))

            elif isinstance(k, bytes):
                result.append(tag_bytes + hexlify(k))

            elif isinstance(k, list):
                lexdump(k, result)

            else:
                raise TypeError("%r not supported" % type(k))

        return b'+'.join(result)+b'+'

    
    def lexload(key):
        result = []
        
        for k in key.split(b'+')[:-1]:
            tag = k[0:1]
            raw = unhexlify(k[1:])

            if tag == tag_str:
                result.append(str(raw, 'utf8'))

            elif tag == tag_nint or tag == tag_pint:
                result.append(load64(raw)[0])
            
            elif tag == tag_bytes:
                result.append(raw)

        return result

    
    return lexdump, lexload
