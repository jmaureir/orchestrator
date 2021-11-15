import base64
import dill

# AbstractPipeline

class AbstractPipeline: 
    def __repr__(self):
        return "<Pipeline(id=%s, name='%s', owner='%s', version=%d, active='%s'>" % (
            str(self.id), self.name, self.owner_id, self.version, self.active
        )
                
    def getFunction(self):
        # include decryption
        return dill.loads(base64.b64decode(self.impl_fn))
        
    def setActive(self, state):
        self.active = state
        return True
    
    def isActive(self):
        return self.active
        
    def asJson(self):
        impl_fn_b64 = ""
        if self.impl_fn is not None:
            impl_fn_b64 = base64.b64encode(self.impl_fn).decode("utf8")
            
        return {
            "id"          : self.id,
            "name"        : self.name,
            "owner_id"    : self.owner_id,
            "creation"    : self.creation,
            "version"     : self.version,
            "tags"        : self.tags,
            "changed"     : self.changed,
            "active"      : self.active,
            "impl_fn"     : impl_fn_b64
        }
        
    
    # arguments are volatile. only used to set arguments for execution 
    def setArguments(self, args):
        self.args = args
        
    # keywords arguments are volatile. only used to set arguments for execution 
    def setKeywordArguments(self, kw_args):
        self.kw_args = kw_args
    
    def getArguments(self):
        if hasattr(self,"args"):
            return self.args
        return []
    
    def getKeywordArguments(self):
        if hasattr(self,"kw_args"):
            return self.kw_args
        return {}
    
    @classmethod
    def fromJson(cls, json):
        pipeline = cls()
        pipeline.id          = json["id"]
        pipeline.name        = json["name"]
        pipeline.owner_id    = json["owner_id"]
        pipeline.creation    = json["creation"]
        pipeline.version     = json["version"]
        pipeline.tags        = json["tags"]
        pipeline.changed     = json["changed"]
        pipeline.active      = json["active"]
        pipeline.impl_fn     = base64.b64decode(json["impl_fn"].encode("utf8"))
        
        return pipeline
        
