# 
# Async decorator
# with threads and processes
#
import multiprocessing as mp
import jsonpickle
import ctypes
import random 
import string 
from .argument import Argument
import psutil
import signal
import threading
import os

import sys, traceback

from promise import Promise

class TimeoutError(RuntimeError):
    pass

class ProcessAsyncCall(object):

    pool_cnt = {}

    def __init__(self, fnc, pool_size, callback = None):
        
        self.p_id      = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)]) 

        self.single    = False

        if pool_size is None:
            self.single    = True
            self.pool_size = 1
        else:
            self.pool_size = pool_size

        self.Callable  = fnc
        self.Callback  = callback
        self.Result    = None
        self.Manager = mp.Manager()
        self.pool      = None

        self.parent_pid = os.getpid()

    def cancel(self, sig=signal.SIGTERM):
        try:
            parent = psutil.Process(self.parent_pid)
        except psutil.NoSuchProcess:
              return
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(sig)
        
    def getPool(self):
        if self.Callable.__name__ not in self.pool_cnt:
            if self.Manager is None:
                self.Manager = mp.Manager()
            self.pool_cnt[self.Callable.__name__] = {"queue": self.Manager.list()  ,"running" : self.Manager.list() }
        return self.pool_cnt[self.Callable.__name__]

    def __call__(self, *args, **kwargs):
        self.Result  = self.Manager.Value(ctypes.c_wchar,'')
 
        if self.single :
            self.proc = mp.Process(target = self.run, args = args, kwargs = kwargs)
            #self.proc.daemon = True
            self.proc.start()
        else:
            if self.Callable.__name__ not in self.pool_cnt:
            #   print("creating pool record",self.pool_size)
            	self.pool_cnt[self.Callable.__name__] = {"queue": self.Manager.list()  ,"running" : self.Manager.list() }

            pool = self.getPool()
        
            self.proc = mp.Process(target = self.run, name = self.Callable.__name__, args = args, kwargs = kwargs)

            self.lock = self.Manager.Lock()

            if len(pool["running"]) < self.pool_size:
                #print("running",self.p_id)
                pool["running"].append(self.p_id)
                #print(len(pool["queue"]),len(pool["running"]))

                self.proc.start()

            else:
                #print("queueing ",self.p_id)
                self.lock.acquire()
                pool["queue"].append(self.lock) 
                #print(len(pool["queue"]),len(pool["running"]))

                def start_proc(proc):
                    #print("start proc adquire", self.p_id)
                    try:
                        proc.lock.acquire()
                        proc.lock.release()
                    except Exception as e:
                        print("error trying to acquire lock.",e)

                    #print("start proc released", self.p_id)
                    pool = proc.getPool()
                    pool["running"].append(self.p_id)
                    proc.proc.start()
                    return

                t = threading.Thread(target=lambda p: start_proc(p) , args=(self,) )
                t.start()
                t.join()

        return self
    
    def then(self, fn):
        response = self.get()
        return fn(*response)

    def wait(self, timeout = None):        
        if self.proc.is_alive():
            self.proc.join(timeout)
            if self.proc.is_alive():
                raise TimeoutError()
        return True

    def get(self, default = None):
        if self.Result.value == '':
            self.wait()
       
        if self.proc.is_alive():
            self.proc.join()
            
        self.proc.terminate()
        
        self.proc.close()

        ret = default
        try:
            ret = jsonpickle.decode(self.Result.value)
        except Exception as e:
            print("error decoding result value",e)
            pass

        self.Result = None
        if self.single:
            self.Manager.shutdown()
            self.Manager = None
        else:
            pool = self.getPool()
            if len(pool["running"])==0 and self.Manager is not None and len(pool["queue"])==0:
                #print("shutting down manager")
                # kill the manager when all running processes are done
                self.Manager.shutdown()
                self.Manager = None
                del self.pool_cnt[self.Callable.__name__] 

        return ret

    def run(self, *args, **kwargs):
        try:
            result = self.Callable(*args, **kwargs)
            try: 
                packed_result = jsonpickle.encode(result)
                self.Result.value = "%s" % packed_result
            except Exception as e:
                print("error calling function:",e)
                try:
                    self.lock.release()
                except:
                    pass
                raise e
                
            if self.Callback:
                self.Callback(self.Result)
        except Exception as e:
            print(e, args, kwargs)
            try:
                self.lock.release()
            except:
                pass
            raise e

        try:
            self.lock.release()
            self.lock = None
        except:
            pass

        if not self.single:
            pool = self.getPool()
            #print("removing ",self.p_id)
            pool["running"].remove(self.p_id) 
            #print(len(pool["queue"]),len(pool["running"]))

            pool_len = len(pool["running"])
            if pool_len < self.pool_size:
                if len(pool["queue"])>0:
                    proc_avai = self.pool_size - pool_len
                    #print("releasing %d processes" % proc_avai)
                    for i in range(0,proc_avai):
                        try:
                            l = pool["queue"].pop(0)
                            l.release()
                        except Exception as e:
                            print("error releasing the lock in dequeue",e)

        return
    
class ThreadAsyncCall(object):
    def __init__(self, fnc, callback = None):
        self.Callable = fnc
        self.Callback = callback
        self.Result = None
        
    def __call__(self, *args, **kwargs):
        self.Thread = threading.Thread(target = self.run, name = self.Callable.__name__, args = args, kwargs = kwargs)
        self.Thread.start()
        return self

    def wait(self, timeout = None):
        self.Thread.join(timeout)
        
        if self.Thread.is_alive():
            raise TimeoutError()
        else:
            return self.Result

    def get(self):
        if self.Result is None:
            self.wait()

        self.Thread.join()
        del self.Thread

        while isinstance(self.Result, (ThreadAsyncCall, ProcessAsyncCall, Argument)):
            self.Result = self.Result.get()
           
        if isinstance(self.Result, RuntimeError):
            raise(self.Result)
 
        return self.Result

    def run(self, *args, **kwargs):
        try:
            self.Result = self.Callable(*args, **kwargs)
            if self.Callback:
                self.Callback(self.Result)
        except Exception as e:
            print(e, args, kwargs)
            traceback.print_exc(file=sys.stdout)

class ThreadAsyncMethod(object):
    def __init__(self, fnc, callback=None):
        self.Callable = fnc 
        self.Callback = callback

    def __call__(self, *args, **kwargs):
        return ThreadAsyncCall(self.Callable, self.Callback)(*args, **kwargs)

class ProcessAsyncMethod(object):
    def __init__(self, fnc, callback=None, pool_size = 1):
        self.pool_size = pool_size
        self.Callable  = fnc 
        self.Callback  = callback

    def __call__(self, *args, **kwargs):
        return ProcessAsyncCall(self.Callable, self.pool_size, self.Callback)(*args, **kwargs)

def ProcessAsync(arg = None, callback = None):
    if isinstance(arg, int):
        def ProcessAsyncWrapper(fnc = None,  callback = None):
            if fnc == None:
                def AddAsyncCallback(fnc):
                    return ProcessAsyncMethod(fnc, callback, arg)
                return AddAsyncCallback
            else:
                return ProcessAsyncMethod(fnc, callback, arg)
        return ProcessAsyncWrapper
    else:
        return ProcessAsyncMethod(arg, callback, None)

def Async(fnc = None, callback = None):
    if fnc == None:
        def AddAsyncCallback(fnc):
            return ThreadAsyncMethod(fnc, callback)
        return AddAsyncCallback
    else:
        return ThreadAsyncMethod(fnc, callback)

class AsyncDummy(object):
    def __init__(self, function, result):
        self.function = function
        self.result = result
    def wait(self):
        return self.result
    def get(self):
        return self.result