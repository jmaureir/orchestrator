# Executor
import sys
import io
from io import StringIO
from uuid import uuid4
import base64
import dill
import jsonpickle
import inspect
import time
from datetime import date, datetime, timezone
from tzlocal import get_localzone 
import traceback

import sqlalchemy as sal
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

from ..exceptions import ImplementationIsNotAFunction, PipelineExecutionError
from ..base import Argument, Async, asJob
from ..base import Observable
from ..orchestrator import OrchestratorAccess

from . import AbstractExecutor
from .Events import *
from .executePipelineAsJob import *

Base = declarative_base()

class Executor(AbstractExecutor, Base, Observable):
    
    __tablename__ = 'executions'
    
    id             = sal.Column('id', sal.Integer, primary_key=True, nullable=False)
    name           = sal.Column('name', sal.String)
    version        = sal.Column('version', sal.Integer)
    owner_id       = sal.Column('owner_id', sal.String)
    uuid           = sal.Column('uuid', sal.String)
    sch_evt_uuid   = sal.Column('sch_evt_uuid', sal.String)
    creation       = sal.Column('creation', sal.DateTime(timezone=True),server_default=func.now())
    start_ts       = sal.Column('start_ts', sal.DateTime(timezone=True))
    end_ts         = sal.Column('end_ts', sal.DateTime(timezone=True))
    exec_time      = sal.Column('exec_time', sal.Interval(second_precision=3))
    state          = sal.Column('state', sal.Integer)    
    pipeline_fn    = sal.Column('pipeline_fn', sal.TEXT)
    pipeline_args  = sal.Column('pipeline_args', sal.TEXT)
    pipeline_ret   = sal.Column('pipeline_ret', sal.TEXT)
    output         = sal.Column('output', sal.TEXT)
    error          = sal.Column('error', sal.TEXT)
    
    def __init__(self, em, pipeline, local=True, cores=1, partition=None, memory=None):
        
        Observable.__init__(self)
        # non persistent attributes
        self.em          = em
        self.pipeline    = pipeline
        self.handler     = None
        self.job_handler = None
        self.cores       = cores
        self.local_job   = local
        self.partition   = partition
        self.memory      = memory
        
        # persistent attributes
        self.name        = pipeline.name
        self.version     = pipeline.version
        self.owner_id    = pipeline.owner_id
        self.creation    = datetime.now(timezone.utc).astimezone(get_localzone())
        self.pipeline_fn = pipeline.impl_fn
        self.uuid        = str(uuid4())
        self.state       = 1  # created
        
        if hasattr(pipeline,"scheduled_event_uuid"):
            self.sch_evt_uuid = pipeline.scheduled_event_uuid
        
        if not self.em.saveObject(self):
            print("error saving executor")

    def setLocalJob(self, b):
        self.local_job = b

    def setCores(self, cores):
        self.cores = cores

    def refresh(self):
        self.em.refreshObject(self)        
    
    def run(self, *args, **kwargs):
        name = self.pipeline.name
        
        orch_access = OrchestratorAccess(self.em.owner.owner, self.pipeline)
        
        pipeline_fn = self.pipeline.getFunction()

        pipeline_args = {
            'args': args, 
            'kwargs': kwargs
        }
        
        self.pipeline_args = base64.b64encode(dill.dumps(pipeline_args))
        self.state         = 2  # initialized        
        
        if not self.em.saveObject(self):
            print("error saving executor")
        
        # add this executor to the executor manager active list
        self.em.active[self.uuid] = self 

        if inspect.isfunction(pipeline_fn):
            
            result = None
            
            output = StringIO()
            error  = StringIO()
            
            def oprint(*args):
                print(*args, file=output)
                
            def eprint(*args):
                print(*args, file=error)

            # trigger execution start event
            self.actionPerformed(ExecutionStarted())
            
            start_ts = datetime.now(timezone.utc).astimezone(get_localzone())
            oprint("Pipeline Execution")
            oprint("pipeline    : %s" % name)
            oprint("pipeline version  : %d" % self.pipeline.version)
            oprint("cores       : %d" % self.cores)
            oprint("start time  : ",start_ts)
            oprint("arguments   : %s %s" % (str(args), str(kwargs)))

            # execute the pipeline as a process in order to wait for the result 
            # or eventually cancel the execution
            
            @Async
            def execute_pipeline():

                # execution process which trigger the pipeline as job 
                # and wait for it to finish
                
                try:

                    returned_arg = None
                    
                    if self.local_job:
                        oprint("executing pipeline as process")
                        self.job_handler = execute_pipeline_as_local(self.cores, pipeline_fn, orch_access, args, kwargs )
                    else:
                        oprint("executing pipeline as job")
                        # TODO: until providing a better orch_access to be used from compute nodes
                        orch_access.pipeline = None
                        orch_access.manager = None

                        self.job_handler = execute_pipeline_as_job(self.cores, pipeline_fn, orch_access, args, kwargs )

                    oprint("waiting for pipeline to finish")
                    
                    self.start_ts = start_ts
                    self.state    = 3  # running

                    if not self.em.saveObject(self):
                        eprint("error saving executor")
                    
                    # wait for the result (the second get is to get the returned argument value)
                    if self.local_job:
                        returned_arg = self.job_handler.get().get()
                    else:
                        returned_arg = self.job_handler.get()
                    
                    print("returned arg:",returned_arg)

                    success, result, b64_output, b64_error = returned_arg

                    if success:
                        oprint("pipeline execution successfully finished")
                        self.state    = 4  # finished
                    else:
                        oprint("pipeline execution failed")
                        self.state    = 5  # error
                        
                    self.pipeline_ret = base64.b64encode(dill.dumps(result))
                    self.em.active[self.uuid] = self 
                    
                    self.actionPerformed(ExecutionFinished())
                    
                    end_ts = datetime.now(timezone.utc).astimezone(get_localzone())
                    self.end_ts = end_ts
                    self.exec_time = end_ts - start_ts
    
                    oprint("execution output start")
                    oprint(base64.b64decode(b64_output).decode('utf8'))
                    eprint(base64.b64decode(b64_error).decode('utf8'))

                    oprint("execution output end")

                    oprint("pipeline return value")
                    oprint(result)

                    oprint("pipeline execution complete")

                    output.seek(0)
                    error.seek(0)

                    self.output   = base64.b64encode(output.read().encode('utf8'))
                    self.error    = base64.b64encode(error.read().encode('utf8'))

                    if not self.em.saveObject(self):
                        eprint("error saving executor")
                    
                    # remove the executor from active list in the executor manager
                    del self.em.active[self.uuid]
                    
                    return result
                
                except Exception as e:
                    self.state    = 5  # error
                    self.error    = "%s" % e

                    print("exception when running the pipeline",e)
                    st = io.StringIO()
                    traceback.print_exc(file=st)
                    st.seek(0)
                    st_str = st.read()

                    eprint(e)
                    eprint(st_str)
                    
                    output.seek(0)
                    error.seek(0)

                    self.output   = base64.b64encode(output.read().encode('utf8'))
                    self.error    = base64.b64encode(error.read().encode('utf8'))

                    if not self.em.saveObject(self):
                        print("error saving executor")

                    del self.em.active[self.uuid]
                    
                    return e
                
            try:
                # execute pipeline as job
                self.handler = execute_pipeline()

            except Exception as e:
                self.output   = None
                self.error    = "%s" % e
            
            return self
        else:
            raise ImplementationIsNotAFunction(name)

    def cancel(self):
        if self.handler is not None:
            if self.job_handler is not None:
                self.job_handler.cancel()
            
    def serialize(self):
        s_output = None
        if self.output is not None:
            s_output = self.output.decode("utf8")
            
        s_error = None
        if self.error is not None:
            s_error = self.error.decode("utf8")
            
        s_args = None
        if self.pipeline_args is not None:
            s_args = self.pipeline_args.decode("utf8")
            
        obj = {
            "id"            : self.id,
            "name"          : self.name,
            "version"       : self.version,
            "owner_id"      : self.owner_id,
            "uuid"          : self.uuid,
            "creation"      : self.creation,
            "start_ts"      : self.start_ts,
            "end_ts"        : self.end_ts,
            "exec_time"     : self.exec_time,
            "state"         : self.state,
            "pipeline_fn"   : self.pipeline_fn.decode("utf8"),            
            "pipeline_args" : s_args,
            "output"        : s_output,
            "error"         : s_error
        }
        # dict is serialized as b64 dill
        sobj = { "uuid": self.uuid, "sobj": base64.b64encode(dill.dumps(obj)).decode("utf8") }

        return sobj
