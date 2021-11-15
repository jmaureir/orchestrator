
# PipelineCatalog    
import base64
import dill
import pandas as pd

from sqlalchemy.sql import func

from ..base.db import DataBaseBackend
from ..exceptions import InitializeError, PipelineAlreadyRegistered, PipelineNotFound, PipelineNotSavedInCatalog
from . import Pipeline

class PipelineCatalog(DataBaseBackend):

    def __init__(self, manager, db_conn_str = "sqlite:///orchestrator.sqlite"):
        
        super().__init__(db_conn_str)
        
        self.manager = manager
        
        if not self.initialize(Pipeline):
            raise InitializeError(Pipeline.__tablename__)
            
    def isRegistered(self,name,**kw_args):
        result = self.getObjects(Pipeline, name=name,**kw_args)
        if len(result)>0:
            return True
        return False

    def register(self, name, owner_id, pipeline_fn, tags=[], new_version = False ):
        if not new_version:
            if not self.isRegistered(name):
            
                #TODO: encrypt serialization with owner_key
                pipeline_serialized = base64.b64encode(dill.dumps(pipeline_fn))

                tags_str = ",".join(tags)

                new_pipeline = Pipeline(
                    name          = name,
                    owner_id      = owner_id,
                    version       = 1,
                    tags          = tags_str,
                    active        = False,
                    impl_fn       = pipeline_serialized
                )                
                if self.saveObject(new_pipeline):
                    saved_pipeline = self.getObjects(Pipeline,
                        name      = name, 
                        owner_id  = owner_id,
                        version   = 1,
                        tags      = tags_str,
                        active    = False
                    )
                    return saved_pipeline
                else:
                    raise PipelineNotSavedInCatalog(name)
            raise PipelineAlreadyRegistered(name)   
            
        else:
            if self.isRegistered(name):
                df = pd.read_sql("SELECT max(version) as version FROM pipelines where name=='%s'"% name, con=self.getEngine())
                
                cur_version = int(df.version.values[0])
                new_version = cur_version+1
            
                #TODO: encrypt serialization with owner_key
                pipeline_serialized = base64.b64encode(dill.dumps(pipeline_fn))

                tags_str = ",".join(tags)

                new_pipeline = Pipeline(
                    name          = name,
                    owner_id      = owner_id,
                    version       = new_version,
                    tags          = tags_str,
                    active        = False,
                    impl_fn       = pipeline_serialized
                )

                if self.saveObject(new_pipeline):
                    saved_pipeline = self.getObjects(Pipeline,
                        name      = name, 
                        owner_id  = owner_id,
                        version   = new_version,
                        tags      = tags_str,
                        active    = False
                    )

                    return saved_pipeline[0]
                else:
                    raise PipelineNotSavedInCatalog(name)
    
            raise PipelineAlreadyRegistered(name)   
        
    def get(self, *args, **kwargs):
        results = self.getObjects(Pipeline,*args,**kwargs)

        # assign for each object the manager where they come from
        for i in range(0,len(results)):
            results[i].catalog = self
            results[i].manager = self.manager
            
        if len(results)==0:        
            raise PipelineNotFound("%s" % kwargs)    
        return results
    
    def deactivateAll(self, name):
        return self.updateObjects(Pipeline, Pipeline.name == name, active = False )>0
