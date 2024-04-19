# RemoteProcedureNotificationManager    
import base64
import pandas as pd
import getpass
from uuid import uuid4
import json

from sqlalchemy.sql import func

from ..base import Observable
from ..base.db import DataBaseBackend
from ..exceptions import InitializeError, NoActivePipelineRegistered, SubscriberNotRegistered, NotificationNotRegistered
from .RemoteProcedureNotification import RemoteProcedureNotification

from .RemoteProcedureNotificationSubscriber import *

class RemoteProcedureNotificationManager(DataBaseBackend,Observable):

    def __init__(self, manager, db_conn_str = "sqlite:///orchestrator.sqlite"):
        
        super().__init__(db_conn_str)
        
        self.manager = manager
        
        if not self.initialize(RemoteProcedureNotification):
            raise InitializeError(RemoteProcedureNotification.__tablename__)

        if not self.initialize(RemoteProcedureNotificationSubscriber):
            raise InitializeError(RemoteProcedureNotificationSubscriber.__tablename__)

    def subscribePipeline(self, label, pipeline_name, owner_id=None):
        uuid = str(uuid4())
        
        if owner_id is None:
            owner_id = getpass.getuser()

        # verify whether the pipeline exists 
        pipeline = None
        try:
            pipeline = self.manager.getActivePipeline(pipeline_name)
        except Exception as e:
            raise NoActivePipelineRegistered(pipeline_name)

        new_subscriber = RemoteProcedureNotificationSubscriber(
            label          = label,
            owner_id       = owner_id,
            uuid           = uuid,
            pipeline_name  = pipeline_name
        )

        if self.saveObject(new_subscriber):
            saved_subscriber = self.getObjects(RemoteProcedureNotificationSubscriber,
                label     = label, 
                owner_id  = owner_id,
                uuid      = uuid
            )
            if len(saved_subscriber)==1:
                return saved_subscriber[0]
            else:
                raise SubscriberNotRegistered(label)
        else:
            raise SubscriberNotRegistered(label)         

    def getSubscribedPipelines(self, label, **kw_args):
        result = self.getObjects(RemoteProcedureNotificationSubscriber, label=label,**kw_args)
        if len(result)>0:
            return result
        return []

    def getSubscriptionsByPipeline(self, pipeline_name, **kw_args):
        result = self.getObjects(RemoteProcedureNotificationSubscriber, pipeline_name=pipeline_name,**kw_args)
        if len(result)>0:
            return result
        return []

    def unsubscribrePipeline(self, uuid):
        result = self.getObjects(RemoteProcedureNotificationSubscriber, uuid=uuid)
        if len(result)>0:
            for subscriber in result:
                self.destroyObject(subscriber)
            return True

        return False

    def getNotifications(self, label, **kw_args):
        result = self.getObjects(RemoteProcedureNotification, label=label,**kw_args)
        if len(result)>0:
            return result
        return []

    def getLastNotification(self,label,**kw_args):
        lst = self.getNotifications(label,**kw_args)
        if len(lst)>0:
            return max(lst)
        return None
    
    def createNotification(self, label, owner_id=None, data={}):
        uuid = str(uuid4())
        
        if owner_id is None:
            owner_id = getpass.getuser()
            
        new_notification = RemoteProcedureNotification(
            label    = label,
            owner_id = owner_id,
            uuid     = uuid,
            data     = json.dumps(data)
        )

        if self.saveObject(new_notification):
            saved_notification = self.getObjects(RemoteProcedureNotification,
                label     = label, 
                owner_id  = owner_id,
                uuid      = uuid
            )
            if len(saved_notification)==1:

                notification = saved_notification[0]

                subscribers = self.getSubscribedPipelines(label)
                for subscriber in subscribers:
                    pipeline = self.manager.getActivePipeline(subscriber.pipeline_name)
                    if pipeline is not None:
                        executor = None
                        kw_args = {}
                        kw_args["rpn_data"]=data

                        if "event" in data:
                            if data["event"]=="finished":
                                print("chained trigger for finished execution")
                                result = dill.loads(base64.b64decode(data["result"]))
                                if isinstance(result,list) or isinstance(result,tuple):
                                    executor = self.manager.execute(pipeline, result, **kw_args)
                                elif isinstance(result,dict):
                                    kw_args.update(result)
                                    executor = self.manager.execute(pipeline, **kw_args)
                                else:
                                    executor = self.manager.execute(pipeline, result, **kw_args)

                            elif data["event"]=="failed":
                                print("chained trigger for failed execution")
                                ex = dill.loads(base64.b64decode(data["result"]))
                                executor = self.manager.execute(pipeline, ex, **kw_args)

                            else:
                                # different event
                                executor = self.manager.execute(pipeline, result, **kw_args)
                        else:
                            # normal rpn. rpn_data comes into the kw_args

                            executor = self.manager.execute(pipeline, **kw_args)

                        print("rpn triggering %s" % pipeline)
                        print("execution_id: %s" % executor.getExecutionId())
                        notification.addTrigger(subscriber.getPipeline())
                        self.saveObject(notification)
                    else:
                        print("no pipeline associated to notification")

                return notification
            else:
                raise NotificationNotRegistered(label)
        else:
            raise NotificationNotRegistered(label)
