import base64
import dill
import json

# AbstractRemoteProcedureNotification

class AbstractRemoteProcedureNotification: 
    
    def addTrigger(self, pipeline_name):

        if not hasattr(self, 'data'):
            self.data = {}
        else:
            if isinstance(self.data, str):
                self.data = json.loads(self.data)

        if "triggers" not in self.data:
            self.data["triggers"] = []
            
        self.data["triggers"].append(pipeline_name)
  
        self.data = json.dumps(self.data)

    def getData(self):
        if not hasattr(self, 'data'):
            self.data = {}
            
        return json.loads(self.data)
    
    def __repr__(self):
        return "<RemoteProcedureNotification(id=%s, label='%s', owner='%s', uuid='%s', creation=%s, data='%s'>" % (
            str(self.id), self.label, self.owner_id, self.uuid, self.creation, self.data
        )

    def __gt__(self, other):
        return self.creation > other.creation
    
    def asJson(self):
        return {
            "id"            : self.id,
            "label"         : self.label,
            "owner_id"      : self.owner_id,
            "uuid"          : self.uuid,
            "creation"      : self.creation,
            "data"          : json.dumps(self.data)
        }
        
    @classmethod
    def fromJson(cls, json_obj):
        obj = cls()
        obj.id          = json_obj["id"]
        obj.label       = json_obj["label"]
        obj.owner_id    = json_obj["owner_id"]
        obj.creation    = json_obj["creation"]
        obj.uuid        = json_obj["uuid"]
        obj.data        = json.loads(json_obj["data"])
        
        return obj
