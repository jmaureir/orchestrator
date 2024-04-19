from . import AVROCodec
import subprocess
import time
import os
import sys
import socket
import importlib
import codecs

import select
import os
import re
import jsonpickle as jp

from taskit.backend import BackEnd, ADMIN_TASKS
from taskit.log import FileLogger, DEBUG, INFO, ERROR

from promise import Promise
from contextlib import closing

def function_caller(arg):
    import dill, types, traceback, sys

    try:
        func_name = arg["name"]
        f_args = arg["args"]
        func = dill.loads(arg["func"])
        r = func(*f_args)
        return r
    except Exception as e:
        print("exception raised when calling function by the worker",e)
        traceback.print_exc(file=sys.stdout)
        return e
    return None

def error_maker():
    assert False, 'Why ever did you call this!?'

class Worker():
    call = None

    def __init__(self):
        #print("Worker constructor")
        pass

    def __del__(self):
        #print("Worker destructor") 
        pass

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def run(self):

        my_host = os.uname()[1]
        tasks = {'function_caller': function_caller, 'get-error': error_maker }
        tasks.update(ADMIN_TASKS)

        port = self.get_free_port()
        job_id = os.getenv("SLURM_JOB_ID")
        step = os.getenv("SLURM_STEP_ID")
        if step == None:
            step = 0

        #home_pwd=os.environ["HOME"]
        #f = open("%s/tmp/backend-%s.%s" % (home_pwd, job_id,step), "w+")
        #log = FileLogger(f,[DEBUG, INFO, ERROR])

        #backend = BackEnd(tasks,host=my_host, port=port, codec=bupacl.orch.base.AVROCodec, tracebacks=True, logger=log)
        backend = BackEnd(tasks,host=my_host, port=port, codec=bupacl.orch.base.AVROCodec, tracebacks=True)

        print("JOBID: %s STEP: %s PORT: %d HOST: %s" % (os.getenv("SLURM_JOB_ID"),step,port,os.getenv("SLURMD_NODENAME")),flush=True)

        backend.main()

        #print("Backend finished")
