import base64
import dill
import inspect

# AbstractScheduledEvent

class AbstractScheduledEvent(object):
    @classmethod
    def fromJson(cls, json):
        obj = cls()
        for k,v in json.items():
            setattr(obj, k, v)
        return obj
    
    def setArguments(self,pipeline_args):
        self.args = base64.b64encode(dill.dumps(pipeline_args))
        
    def setKeywordArguments(self,pipeline_kw_args):
        self.kw_args = base64.b64encode(dill.dumps(pipeline_kw_args))
        
    def getArguments(self):
        if self.args is not None:
            return dill.loads(base64.b64decode(self.args))
        else:
            return ()

    def getKeywordArguments(self):
        if self.kw_args is not None:
            return dill.loads(base64.b64decode(self.kw_args))
        else:
            return {}
    
    def __repr__(self):
        return "<ScheduledEvent[name=%s, owner=%s, uuid=%s, trigger_time=%s, recurrency=%s, active=%s, pipeline=%s]>" % (
            self.name,
            self.owner_id,
            self.uuid,
            self.trigger_time,
            self.recurrency,
            self.active,
            self.pipeline
        )
