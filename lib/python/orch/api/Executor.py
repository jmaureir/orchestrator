# Executor skeleton
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

from ..exceptions import *

class Executor():
    def __init__(self, orch_api, pipeline, local=True, cores=1, partition=None, memory=None):
        self.orch_api    = orch_api
        self.pipeline    = pipeline
        self.cores       = cores
        self.local_job   = local
        self.memory      = memory
        self.partition   = partition
   
    def setLocalJob(self, b):
        self.local_job = b

    def setCores(self, cores):
        self.cores = cores

    def run(self, *args, **kwargs):
        post_data = {
            "name"     : self.pipeline.name,
            "version"  : self.pipeline.version,
            "cores"    : self.cores,
            "asJob"    : self.local_job,
            "args"     : jsonpickle.encode( args ),
            "kwargs"   : jsonpickle.encode( kwargs )
        }
        
        if self.memory is not None:
            post_data["memory"] = self.memory

        if self.partition is not None:
            post_data["partition"] = self.partition

        print(post_data)
            
        try:
            json_response = self.orch_api.post("/execute",json=post_data)
            if json_response["code"] == 201:
                self.execution_id = json_response["execution_id"]
                return self.execution_id
            else:
                raise PipelineExecutionError(json_response)
        except Exception as e:
            raise e

    def cancel(self):
        pass
