import base64
import dill
import inspect

# AbstractExecutor

class AbstractExecutor(object):
    @classmethod
    
    def fromJson(cls, json):
        obj = cls()
        
        for k,v in json.items():
            setattr(obj, k, v)
        
        obj.handler     = None
        obj.job_handler = None    
        
        return obj

    def asJson(self):
        if self.end_ts is None:
            end_ts_str = "None"
        else:
            end_ts_str = self.end_ts.strftime("%m/%d/%Y %H:%M:%S")

        if self.exec_time is None:
            exec_time_sec = None
        else:
            exec_time_sec = self.exec_time.total_seconds()

        json_obj = {
            "id"            : self.id,
            "name"          : self.name,
            "version"       : self.version,
            "owner_id"      : self.owner_id,
            "uuid"          : self.uuid,
            "creation"      : self.creation.strftime("%m/%d/%Y %H:%M:%S"),
            "start_ts"      : self.start_ts.strftime("%m/%d/%Y %H:%M:%S"),
            "end_ts"        : end_ts_str,
            "exec_time"     : exec_time_sec,
            "state"         : self.state
        }
        return json_obj

    def getOutput(self):
        if self.output is not None:
            return base64.b64decode(self.output).decode("utf8")
        return None

    def getReturnValue(self):
        if self.pipeline_ret is not None:
            return dill.loads(base64.b64decode(self.pipeline_ret))
        return None
    
    def getErrors(self):
        if self.error is not None:
            return base64.b64decode(self.error).decode("utf8")
        return None
    
    def getArguments(self):
        return dill.loads(base64.b64decode(self.pipeline_args.encode("utf8")))
    
    def getFunction(self):
        # return the function as a function handler
        return dill.loads(base64.b64decode(self.pipeline_fn.encode("utf8")))

    def isPreparing(self):
        return self.state == 1 or self.state==2
    
    def isRunning(self):
        return self.state ==3
        
    def isDone(self):
        return self.state == 4 or self.state == 5

    def isSuccessful(self):
        return self.state == 4
    
    def isFailed(self):
        return self.state == 5
    
    def getExecutionId(self):
        # TODO: assert for empty uuid         
        return self.uuid
    
    def __repr__(self):
        return "<Execution[id=%d, when=%s, exec_time=%s, uuid=%s, state=%d, pipeline_name=%s, version=%d]>" % (self.id, self.creation, self.exec_time, self.uuid, self.state, self.name, self.version)
