import sqlalchemy as sal
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import func
from uuid import uuid4
import base64
import dill

# Remote Procedure Notification
from sqlalchemy.ext.declarative import declarative_base
from .AbstractRemoteProcedureNotification import *
Base = declarative_base()

class RemoteProcedureNotification(AbstractRemoteProcedureNotification,Base):
    __tablename__ = 'remoteprocedurenotification'
    
    id         = sal.Column('id', sal.Integer, primary_key=True, nullable=False)
    label      = sal.Column('label', sal.String)
    owner_id   = sal.Column('owner_id', sal.String)
    uuid       = sal.Column('uuid', sal.String)
    creation   = sal.Column('creation', sal.DateTime(timezone=True),server_default=func.now())
    data       = sal.Column('data', sal.TEXT)
    
    def serialize(self):            
        obj = self.asJson()
        # dict is serialized as b64 dill
        sobj = { "uuid": self.uuid, "sobj": base64.b64encode(dill.dumps(obj)).decode("utf8") }

        return sobj
