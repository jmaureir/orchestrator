name="orch"
version="1.1"
author="Juan Carlos Maureira"

from .ActionEvent import ActionEvent
from .ActionListener import ActionListener
from .Observable import Observable
from .SerializedObject import *
from ._async import *
from .worker import Worker
from .AbstractJob import AbstractJob
from .argument import Argument
from .AbstractApiService import AbstractApiService
from .AbstractApiClient import AbstractApiClient
from .PersistentDict import PersistentDict

from .slurm import SlurmController
from .slurm import *
from .LocalJob import *

# Decorators

def asJob(*f, **arg):  
    def submitJob(job_params, function, *args, **kwargs):
        if "name" not in job_params and "job-name" not in job_params:
            job_params["job-name"] = function.__name__

        job = Job(params = job_params)
        if "verbose" in job_params:
            job.setVerbose(job_params['verbose'])
        else:
            job.setVerbose(False)

        if "output" in job_params:
            job.setJobLog(job_params['output'])

        job.asJob(True)
        ret = job.run(function, *args, **kwargs)
        return ret
    
    params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
    
    if f != () and callable(*f):
        # call without arguments
        def wrapper(*args, **kwargs):
            return submitJob(params, f[0], *args, **kwargs)
        return wrapper
    else:
        def jobFunction(func):
            params = {"ntasks":"1"}
            if len(arg)>0 :
                params.update(arg)

            def wrapper(*args, **kwargs):
                return submitJob(params, func, *args, **kwargs)
            return wrapper
        return jobFunction 

def asLocalJob(*f, **arg):  
    def submitJob(job_params, function, *args, **kwargs):
        if "name" not in job_params and "job-name" not in job_params:
            job_params["job-name"] = function.__name__

        job = LocalJob(params = job_params)
        if "verbose" in job_params:
            job.setVerbose(job_params['verbose'])
        else:
            job.setVerbose(False)

        if "output" in job_params:
            job.setJobLog(job_params['output'])

        job.asJob(True)
        ret = job.run(function, *args, **kwargs)
        return ret
    
    params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
    
    if f != () and callable(*f):
        # call without arguments
        def wrapper(*args, **kwargs):
            return submitJob(params, f[0], *args, **kwargs)
        return wrapper
    else:
        def jobFunction(func):
            params = {"ntasks":"1"}
            if len(arg)>0 :
                params.update(arg)

            def wrapper(*args, **kwargs):
                return submitJob(params, func, *args, **kwargs)
            return wrapper
        return jobFunction 

def asStep(*f, **arg):
    
    def submitJob(job_params, function, *args, **kwargs):
        job_params["job-name"] = function.__name__
        job = Job(params = job_params)
        job.setVerbose(True)
        job.setExclusive(False)
        ret = job.run(function, *args, **kwargs)
        return ret
    
    params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
    
    if f != () and callable(*f):
        # call without arguments
        def wrapper(*args, **kwargs):
            return submitJob(params, f[0], *args, **kwargs)
        return wrapper
    else:
        def jobFunction(func):
            params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
            if len(arg)>0 :
                params.update(arg)
            def wrapper(*args, **kwargs):
                return submitJob(params, func, *args, **kwargs)
            return wrapper
        return jobFunction
      
def asExclusiveStep(*f, **arg):
    def submitJob(job_params, function, *args, **kwargs):
        job_params["job-name"] = function.__name__
        job = Job(params = job_params)
        if "verbose" in job_params:
            job.setVerbose(job_params['verbose'])
        else:
            job.setVerbose(False)

        job.setExclusive(True)
        ret = job.run(function, *args, **kwargs)
        return ret
    
    params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
    
    if f != () and callable(*f):
        # call without arguments
        def wrapper(*args, **kwargs):
            return submitJob(params, f[0], *args, **kwargs)
        return wrapper
    else:
        def jobFunction(func):
            params = {"ntasks":"1","cpus-per-task":1, "mem": "4000M","nodes":1}
            if len(arg)>0 :
                params.update(arg)
            def wrapper(*args, **kwargs):
                return submitJob(params, func, *args, **kwargs)
            return wrapper
        return jobFunction
