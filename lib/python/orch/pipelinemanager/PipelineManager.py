
from flask import request, jsonify
from multiprocessing import Process, Queue, Manager

from datetime import date, datetime, timezone
from tzlocal import get_localzone 
import jsonpickle
import dill
import inspect
import time

from ..base import Observable, ActionListener
from ..orchestrator import OrchestratorManager
from . import PipelineCatalog
from . import ExecutorManager
from ..exceptions import MultipleActivePipelineRegistered, NoActivePipelineRegistered,PipelineExecutionError, PipelineNotRegistered
from .Events import *

class PipelineManager(ActionListener, Observable):
    def __init__(self, owner,db_conn_str = "sqlite:///orchestrator.sqlite"):
        
        super().__init__()
        
        self.owner            = owner
        self.catalog          = PipelineCatalog(self,db_conn_str=db_conn_str)
        self.executor_manager = ExecutorManager(self, db_conn_str=db_conn_str)
        self.db_conn_str = db_conn_str
        
        self.executor_manager.addActionListener(self)
        
    def register(self, name, pipeline_fn, new_version = False):        
        if not new_version:
            if not self.catalog.isRegistered(name):
                try:
                    #TODO: handle the owner in the api
                    new_pipeline = self.catalog.register(name,"jcm",pipeline_fn)
                    return new_pipeline
                except Exception as e:
                    raise e
            # already registered
            return self.catalog.get(name=name)
        else:
            if self.catalog.isRegistered(name):
                try:
                    #TODO: handle the owner in the api
                    new_pipeline = self.catalog.register(name,"jcm",pipeline_fn, new_version = True) 
                    return new_pipeline
                except Exception as e:
                    raise e
            # already registered
            
            return self.catalog.get(name=name)

    def isRegistered(self,name, **kw_args):
        return self.catalog.isRegistered(name,**kw_args)
        
    def get(self, name, **kw_args): 
        if self.catalog.isRegistered(name):
            try:
                pipelines = self.catalog.get(name=name, **kw_args)
                return pipelines
            except Exception as e:
                raise e
        
        # pipele not registered
        return False
    
    def activate(self, pipeline):
        #TODO: check if pipeline is instance of pipeline
        if pipeline.isActive():
            return True

        # deactivate all the pipelines with given name
        self.catalog.deactivateAll(pipeline.name)

        # activate the pipeline
        if pipeline.setActive(True):
            return True
        else:
            return False

    def deactivate(self, pipeline):
        if not pipeline.isActive():
            return True
        # deactivate the pipeline
        if pipeline.setActive(False):
            return True

        return False

    def deactivateAll(self, pipeline):
        return self.catalog.deactivateAll(pipeline.name)

    def execution_status(self, exec_id):
        executor = self.executor_manager.getExecutorByID(exec_id)        
        if executor is not None:            
            return executor.state
        else:
            return None
    
    def get_execution(self, exec_id):
        executor = self.executor_manager.getExecutorByID(exec_id)
        
        if executor is not None:
            return executor
        else:
            return None

    def execute(self, pipeline, *pipeline_args, **pipeline_kwargs):
        try:        
            executor = self.executor_manager.create(pipeline)
            executor.run(*pipeline_args,**pipeline_kwargs)
            return executor
                
        except Exception as e:
            raise e

    def createExecutor(self, pipeline, *args, **kw_args):
        try:        
            executor = self.executor_manager.create(pipeline,*args, **kw_args)
            return executor
                
        except Exception as e:
            raise e
           
    def get_execution_list(self,pipeline_name, **kw_args):
        if self.catalog.isRegistered(pipeline_name):            
            executions = self.executor_manager.getExecutionList(pipeline_name, **kw_args)
            
            if len(executions)>0:                
                return executions
            else:
                return []

        raise PipelineNotRegistered(pipeline_name)
        
    def actionPerformed(self, evt):
        # only forward the event to all listeners
        Observable.actionPerformed(self, evt)
