import sys
import traceback


__all__ = ['close', 'register_close_handler']
_closehandlers = []


def close():
    '''Run any registered exit functions, including closing
    any DbEnv / Db objects
    '''
    while _closehandlers:
        try:
            handler, args, kwargs = _closehandlers.pop()
            handler(*args, **kwargs)
        except:
            print('!!! Error in dbo.close:', file=sys.stderr)
            traceback.print_exc()


def register_close_handler(f, *args, **kwargs):
    '''Register a function to be executed by dbo.close()
    '''
    _closehandlers.append((f, args, kwargs))
    return f



