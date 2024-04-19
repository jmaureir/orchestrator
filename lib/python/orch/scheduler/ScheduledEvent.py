import sqlalchemy as sal
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import func
from uuid import uuid4
import base64
import dill

from ..base import ActionEvent
from . import AbstractScheduledEvent

# ScheduledEvent
# 
# register the scheduled execution of a pipeline identified by name
# the execution time is denoted by trigger time and recurrency
# all scheduled events have a name to identify them easily

Base = declarative_base()

class ScheduledEvent(AbstractScheduledEvent, Base, ActionEvent):
    __tablename__ = 'scheduled_events'
    
    id           = sal.Column('id', sal.Integer, primary_key=True, nullable=False)
    name         = sal.Column('name', sal.String)
    owner_id     = sal.Column('owner_id', sal.String)
    uuid         = sal.Column('uuid', sal.String)
    creation     = sal.Column('creation', sal.DateTime(timezone=True),server_default=func.now())
    tags         = sal.Column('tags', sal.String)
    changed      = sal.Column('changed', sal.DateTime(timezone=True),onupdate=func.now())
    active       = sal.Column('active', sal.Boolean)
    trigger_time = sal.Column('trigger_time', sal.DateTime(timezone=True))
    recurrency   = sal.Column('recurrency', sal.Integer)
    pipeline     = sal.Column('pipeline', sal.String)
    args         = sal.Column('pipeline_args', sal.TEXT)
    kw_args      = sal.Column('pipeline_kw_args', sal.TEXT)
    
    def __init__(self, schm, label):
        self.schm = schm
        self.name = label
        self.uuid = str(uuid4())
        
    def serialize(self):
        s_args = None
        if self.args is not None:
            s_args = self.args.decode("utf8")
            
        s_kw_args = None
        if self.kw_args is not None:
            s_kw_args = self.kw_args.decode("utf8")
            
        obj = {
            "id"            : self.id,
            "name"          : self.name,
            "owner_id"      : self.owner_id,
            "uuid"          : self.uuid,
            "creation"      : self.creation,
            "tags"          : self.tags,
            "changed"       : self.changed,
            "active"        : self.active,
            "trigger_time"  : self.trigger_time,
            "recurrency"    : self.recurrency,            
            "pipeline"      : self.pipeline,
            "args"          : s_args,
            "kw_args"       : s_kw_args
        }
        # dict is serialized as b64 dill
        sobj = { "uuid": self.uuid, "sobj": base64.b64encode(dill.dumps(obj)).decode("utf8") }

        return sobj
