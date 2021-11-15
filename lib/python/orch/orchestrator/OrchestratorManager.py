from datetime import datetime
import getpass
import json

from ..base import ActionListener
from ..scheduler import SchedulerManager, ScheduledEvent
from ..scheduler.Events import *
from ..pipelinemanager.Events import *
from ..pipelinemanager import PipelineManager
from ..exceptions import MultipleActivePipelineRegistered, NoActivePipelineRegistered
from .OrchCredentialManager import OrchCredentialManager
from .RemoteProcedureNotificationManager import RemoteProcedureNotificationManager

class OrchestratorManager(ActionListener):
    def __init__(self,db_conn_str="sqlite:///orchestrator.sqlite"):
        self.owner_id = getpass.getuser()
        self.schm = SchedulerManager(self,db_conn_str=db_conn_str)
        self.pm   = PipelineManager(self,db_conn_str=db_conn_str)
        self.ocm   = OrchCredentialManager(db_conn_str=db_conn_str)
        self.rpnm = RemoteProcedureNotificationManager(self,db_conn_str=db_conn_str)
        
        self.schm.addActionListener(self)
        self.pm.addActionListener(self)
        self.rpnm.addActionListener(self)
        self.db_conn_str = db_conn_str

    def actionPerformed(self, evt):
        
        if isinstance(evt, ExecutePipeline):
            # trigger pipeline from SchedulerManager
            print("OrchestratorManager: trigger", evt)
            try:
                pipeline = self.getActivePipeline(evt.pipeline)
                
                print("executing %s with args: %s kw_args: %s" % (pipeline, evt.args, evt.kw_args))
                # include the scheduled_event_uuid in the pipeline instance 
                # in order ot allow executor to assocaite the execution to the 
                # scheduled event who triggered the execution
                pipeline.scheduled_event_uuid = evt.sch_evt_uuid
                self.execute(pipeline, *evt.args, **evt.kw_args)
                
            except Exception as e:
                print("Error:",e)
        elif isinstance(evt, ExecutionStarted):
            # pipeline manager informing the execution has begun
            print("execution started")
        elif isinstance(evt, ExecutionFinished):
            # pipeline manager informing the execution has finished
            print("execution finished")
        else:
            print("actionEvent",evt)
            
    def getOwner(self):
        return self.owner_id
    
    def register(self, label, pipeline_fn, token_list=None, new_version=False):
        if new_version is False or new_version is None:
            if token_list is not None:
                self.ocm.registerToken(token_list)
        return self.pm.register(label,pipeline_fn,new_version)
    
    def getPipelines(self,name, **kw_args):
        return self.pm.get(name,**kw_args)
    
    def isPipelineRegistered(self,name,**kw_args):
        return self.pm.isRegistered(name,**kw_args)
    
    def getActivePipeline(self, name):
        pipelines = self.pm.get(name, active = True)
        if len(pipelines)==1:
            return pipelines[0]
        elif len(pipelines)>1:
            raise MultipleActivePipelineRegistered(name)
        else:
            raise NoActivePipelineRegistered(name)
    
    def activatePipeline(self, pipeline):
        return self.pm.activate(pipeline)

    def deactivatePipeline(self, pipeline):
        return self.pm.deactivate(pipeline)
    
    def deactivateAll(self, pipeline):
        return self.pm.deactivateAll(pipeline)

    def execute(self, pipeline, *args, **kw_args):
        # verify expiration of credentials and tokens associated with pipeline
        status = self.ocm.checkProcessExpiration(pipeline.name)
        if status is False:
            raise Exception('Credentials or tokens for pipeline has been expired')
        return self.pm.execute(pipeline, *args, **kw_args)

    def createExecutor(self, pipeline, *args, **kw_args):
        return self.pm.createExecutor(pipeline, *args, **kw_args)   
 
    def getExecutionList(self, name, **kw_args):
        return self.pm.get_execution_list(name, **kw_args)
    
    def getExecution(self, exec_id):
        return self.pm.get_execution(exec_id)
    
    def getLastExecution(self, name):
        exec_list = self.getExecutionList(name)
        if len(exec_list)>0:
            last_exec_id = exec_list[-1].getExecutionId()            
            return self.getExecution(last_exec_id)
        return None
    
    def scheduleAt(self, pipeline, label = None, trigger_time=datetime.now().strftime("%H:%M:%S"), recurrency=None, tags=[]):
        return self.schm.scheduleAt(pipeline, label, trigger_time, recurrency, tags)
    
    def cancelScheduledExecution(self, scheduled_event):
        if isinstance(scheduled_event, ScheduledEvent):
            return self.schm.cancelEvent(scheduled_event.uuid)
        return None
    
    def getScheduledExecutions(self, pipeline, **kw_args):
        return self.schm.getScheduledEvents(pipeline,**kw_args)

    def getScheduledExecutionById(self, scheduled_event_id):
        return self.schm.getScheduledEventById(scheduled_event_id)

    def createNotification(self,label, data={}):
        return self.rpnm.createNotification(label,data=data)
        
    def getLastNotification(self,label):
        return self.rpnm.getLastNotification(label)
        
    def getNotificationList(self,label):
        return self.rpnm.getNotifications(label)

    def subscribePipelineNotification(self, label, pipeline_name):

        try:
            pipeline = self.getActivePipeline(pipeline_name)
            if pipeline is not None:
                return self.rpnm.subscribePipeline(label, pipeline.name)

            raise NoActivePipelineRegistered(pipeline_name)
        except Exception as e:
            raise e

    def unsubscribePipelineNotification(self, subscriber):
        return self.rpnm.unsubscribrePipeline(subscriber.uuid)

    def getSubscribedPipelines(self, label):
        return self.rpnm.getSubscribedPipelines(label)

    def getSubscriptionsByPipeline(self, pipeline_name):
        return self.rpnm.getSubscriptionsByPipeline(pipeline_name)

    def stop(self):
        print("stopping OrchestratorManager")
        self.schm.stop()
        
    def putKey(self, key, passphrase=None):
        self.ocm.putKey(key, passphrase)
        
    def getKey(self, label, passphrase=None):
        return self.ocm.getKey(label, passphrase)
        
    def getKeyList(self, active=True):
        return self.ocm.getKeyList(active)
    
    def keyExpiration(self, key):
        return self.ocm.keyExpiration(key)
        
    def setKeyExpiration(self, key, date):   
        self.ocm.setKeyExpiration(key, date)
        
    def getKeyExpirationDate(self, key):
        return self.ocm.getKeyExpirationDate(key)
    
    def getCredentialList(self, active=True):
        return self.ocm.getCredentialList(active)
    
    def credentialExpiration(self, label):   
        return self.ocm.credentialExpiration(label)
            
    def setCredentialExpiration(self, label, date):
        self.ocm.setCredentialExpiration(label, date)
        
    def getCredentialExpirationDate(self, label):
        return self.ocm.getCredentialExpirationDate(label)
        
    def signCredential(self, credential, key=None):
        self.ocm.signCredential(credential, key)  
    
    def putCredential(self, credential, n_unlock=2, shared_users=4):
        self.ocm.putCredential(credential, n_unlock, shared_users)
        
    def getCredential(self, label, decrypt=True, token=None, whom=None):
        return self.ocm.getCredential(label, decrypt, token, whom)
    
    def verifyCredential(self, credential):
        return self.ocm.verifyCredential(credential)
        
    def encryptCredential(self, credential, recipient_key):
        return self.ocm.encryptCredential(credential, recipient_key)
    
    def createToken(self, credential, min_unlock=2, shared_users=4):
        return self.ocm.createToken(credential, min_unlock, shared_users)

    def putToken(self, credential, token_list):
        self.ocm.putToken(credential, token_list)
        
    def registerToken(self, token_list):
        self.ocm.registerToken(token_list)

    def getToken(self, label=None, whom=None, active=True):
        return self.ocm.getToken(label, whom, active)
    
    def assignToken(self, whom, label, exp_date, comment=""):
        return self.ocm.assignToken(whom, label, exp_date, comment)
    
    def getAssignedToken(self, label, whom):
        return self.ocm.getAssignedToken(label, whom)
    
    def signToken(self, label, whom):
        return self.ocm.signToken(label, whom)
        
    def verifyToken(self, token):
        return self.ocm.verifyToken(token)
    
    def getTokenList(self, active=False):
        return self.ocm.getTokenList(active)
    
    def getTokenExpirationDate(self, label, whom):
        return self.ocm.getTokenExpirationDate(label, whom)
    
    def tokenExpiration(self, label, whom=None):
        return self.ocm.tokenExpiration(label, whom)
    
    def getPublicKey(self):
        return self.ocm.getPublicKey()
    
    def getRegisterCredential(self, label, token=None, whom=None):
        return self.ocm.getRegisterCredential(label, token, whom)
    
    def checkProcessExpiration(self, process_name):
        return self.ocm.checkProcessExpiration(process_name)
