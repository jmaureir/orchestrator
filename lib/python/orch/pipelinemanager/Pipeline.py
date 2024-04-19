import sqlalchemy as sal
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import func
import base64
import dill

# Pipeline
from . import AbstractPipeline
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class Pipeline(AbstractPipeline,Base):
    __tablename__ = 'pipelines'
    
    id         = sal.Column('id', sal.Integer, primary_key=True, nullable=False)
    name       = sal.Column('name', sal.String)
    owner_id   = sal.Column('owner_id', sal.String)
    creation   = sal.Column('creation', sal.DateTime(timezone=True),server_default=func.now())
    version    = sal.Column('version', sal.Integer)
    tags       = sal.Column('tags', sal.String)
    changed    = sal.Column('changed', sal.DateTime(timezone=True),onupdate=func.now())
    active     = sal.Column('active', sal.Boolean)
    impl_fn    = sal.Column('impl_fn', sal.TEXT)
    
    catalog    = None
    manager    = None

    def setActive(self, state):
        self.active = state
        if self.catalog is not None:
            self.catalog.deactivateAll(self.name)
            if not self.catalog.saveObject(self):
                return False
        
        return True

    def getExecutions(self,**kw_args):
        if self.manager is not None:
            return self.manager.get_execution_list(self.name, **kw_args)
        return []
