from .AbstractJob import AbstractJob

from threading import Thread, Event
from promise import Promise
import traceback
import tempfile
import dill

from ._async import *

class LocalJob(AbstractJob):
    def __init__(self, params={}):
        super(LocalJob,self).__init__(params)

    def execute(self, function, *args):

        #import psutil
        #max_cores = psutil.cpu_count()

        @ProcessAsync
        def _exec(f, *args):
            self.result = f(*args)
            return self.result

        fn = None
        try:
            fn = function
        except Exception as e:
            print("Error getting the function to call: %s" % function)
            print(e)
            raise(e)

        # unwrap the function if it is wrapped in an AsyncCall
        if isinstance(fn,ThreadAsyncMethod) or isinstance(fn,ProcessAsyncMethod):
            fn = fn.Callable

        self._exec_hdl = _exec(fn, *args)
        return self._exec_hdl
    
