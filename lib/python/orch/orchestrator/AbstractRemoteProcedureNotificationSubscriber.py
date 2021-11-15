import base64
import dill
import json

# AbstractRemoteProcedureNotificationSubscriber

class AbstractRemoteProcedureNotificationSubscriber: 
    
    def getPipeline(self):
        #TODO: get the pipeline from manager
        return self.pipeline_name
    
    def __repr__(self):
        return "<RemoteProcedureNotificationSubscriber(id=%s, label='%s', owner='%s', uuid='%s', creation=%s, pipeline='%s'>" % (
            str(self.id), self.label, self.owner_id, self.uuid, self.creation, self.pipeline_name
        )

    def asJson(self):
        return {
            "id"            : self.id,
            "label"         : self.label,
            "owner_id"      : self.owner_id,
            "uuid"          : self.uuid,
            "creation"      : self.creation,
            "pipeline_name" : self.pipeline_name
        }
        
    @classmethod
    def fromJson(cls, json_obj):
        obj = cls()
        obj.id            = json_obj["id"]
        obj.label         = json_obj["label"]
        obj.owner_id      = json_obj["owner_id"]
        obj.creation      = json_obj["creation"]
        obj.uuid          = json_obj["uuid"]
        obj.pipeline_name = json_obj["pipeline_name"]
        
        return obj
