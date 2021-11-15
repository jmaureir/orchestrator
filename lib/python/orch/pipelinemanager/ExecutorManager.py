# ExecutorManager

from ..base import ActionListener, Observable
from ..base.db import DataBaseBackend
from . import Executor
from ..exceptions import InitializeError,MultipleExecutionIDFound,ExecutionIdNotFound

class ExecutorManager(DataBaseBackend,ActionListener, Observable):
    
    active = {}
    
    def __init__(self,owner,db_conn_str="sqlite:///orchestrator.sqlite"):
        super().__init__(db_conn_str)
        self.owner = owner
        if not self.initialize(Executor):
            raise InitializeError(Executor.__tablename__)

    def create(self,pipeline, *args, **kw_args):
        executor = Executor(self, pipeline, *args, **kw_args)
        executor.addActionListener(self)
        return executor

    def getExecutorByID(self, executor_id):
        # check executor in active list
        if executor_id in self.active:
            executor = self.active[executor_id]
            return executor
        else:
            # executor not active. trying to get it from persistency backend
            try:
                executors = self.getObjects(Executor, uuid=executor_id)                
                if len(executors) == 1:
                    executors[0].em = self
                    executors[0].addActionListener(self)
                    
                    return executors[0]
                elif len(executors) > 1:
                    raise MultipleExecutionIDFound(executor_id)
                else:
                    raise ExecutionIdNotFound(executor_id)
            except Exception as e:
                raise e
            
        return None
    
    def getExecutionList(self, pipeline_name, **kw_args):
        exec_list = []
        # get the active executions first
        for uuid, ex in self.active.items():
            if ex.name == pipeline_name:
                exec_list.append(ex)
                
        # get the executions stored in persistency backend
        try:
            executors = self.getObjects(Executor, name=pipeline_name, **kw_args)
            exec_list = exec_list + executors
        except Exception as e:
            raise e

        return executors
    
    def actionPerformed(self, evt):
        # onyl forward the event to all listeners
        Observable.actionPerformed(self,evt)
        
