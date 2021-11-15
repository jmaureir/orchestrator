
import jsonpickle
from inspect import isfunction
import dill
import base64
import urllib
from datetime import datetime

from .Execution import Execution
from .ScheduledEvent import ScheduledEvent
from .RemoteProcedureNotification import RemoteProcedureNotification
from .Pipeline import Pipeline
from .Executor import *

from ..base import AbstractApiClient
from ..exceptions import PipelineExecutionError, ImplementationIsNotAFunction, APIResponseError, PipelineSchedulingError
from ..orchestrator import RemoteProcedureNotificationSubscriber

from bupacl.credentialmanager.Credential import Credential
from bupacl.credentialmanager.EncryptionKey import EncryptionKey

class Orchestrator(AbstractApiClient):
    def __init__(self, api_url="http://127.0.0.1:8020"):
        super().__init__(api_url)
        
    def status(self):
        return self.get("/")
    
    def stop(self):
        self.get("/stop")
        return True

    def subscribePipelineNotification(self, label, pipeline, *args, **kw_args):
        if isinstance(label,str) and len(label)>0 and isinstance(pipeline,Pipeline):

            subscription_data = {
                "pipeline_name" : pipeline.name
            }

            response = self.post("/rpn/%s/subscribe" % label, json=subscription_data)
            obj_list = []
            if response["code"]==204:
                sobj = response["subscription"]
                obj = RemoteProcedureNotificationSubscriber.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("utf8"))))
                return obj
            elif response["code"]==501:
                e = dill.loads(base64.b64decode(response["exception"].encode("urf8")))
                raise e
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        else:
            raise RuntimeError("you must provide a valid notification label")

    def createNotification(self, label, data={}):
        if isinstance(label,str) and len(label)>0:
            
            response = None
            if len(data.keys())>0:
                params = urllib.parse.urlencode(data)
                response = self.get("/rpn/%s/notify?%s" % (label,params))
            else:
                response = self.get("/rpn/%s/notify" % label)
                
            obj_list = []
            if response["code"]==204:
                sobj = response["notification"]
                obj = RemoteProcedureNotification.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("utf8"))))
                return obj
            elif response["code"]==311:
                # notification not created
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        else:
            raise RuntimeError("you must provide a valid notification label")
    
    def getLastNotification(self,label):
        if isinstance(label,str) and len(label)>0:
            response = self.get("/rpn/%s/last" % label)
            obj_list = []
            if response["code"]==204:
                sobj = response["notification"]
                obj = RemoteProcedureNotification.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("utf8"))))
                return obj
            elif response["code"]==311:
                # notification not created
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        else:
            raise RuntimeError("you must provide a valid notification label")
    
    def getNotificationList(self,label):
        if label is not None or label!="":
            try:
                response = self.get("/rpn/%s/list" % label)
                obj_list = []
                if response["code"]==204:
                    for sobj in response["notification_list"]:
                        obj = RemoteProcedureNotification.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("utf8"))))
                        obj_list.append(obj)
                    return obj_list
                elif response["code"]==311:
                    # no rpn found
                    return []
                else:
                    raise APIResponseError("%d:%s" % (response["code"],response["status"]))
                
            except Exception as e:
                raise e

        raise RuntimeError("you must provide a valid notification label")

    def register(self, name,fn, token_list=None, new_version =False):
        
        if not isfunction(fn):
            raise ImplementationIsNotAFunction(fn)
        
        post_data = {
            "name"        : name,
            "pipeline"    : base64.b64encode(dill.dumps(fn)).decode("utf8"),
            "token_list"  : str(token_list),
            "new_version" : new_version
        }

        return self.post("/register",json=post_data)
    
    def getActivePipeline(self, name):
        pipelines = self.getPipelines(name, active = True)
        if len(pipelines)==1:
            return pipelines[0]
        elif len(pipelines)>1:
            raise MultipleActivePipelineRegistered(name)
        else:
            raise NoActivePipelineRegistered(name)

    def getPipelines(self, name, **kwargs):
        post_data = {
            "name" : name,
        }

        post_data.update(kwargs)
        json_response = self.post("/get",json=post_data)
        
        if "pipeline" in json_response:
            pipe_json_list = json_response["pipeline"]
            pipelines_list = [ Pipeline.fromJson(pipe_json) for pipe_json in pipe_json_list]
            return pipelines_list
        else:
            return None
        
    def activatePipeline(self, pipeline):
        post_data = {
            "name"     : pipeline.name,
            "version"  : pipeline.version
        }
        return self.post('/activate',json=post_data)
    
    def deactivatePipeline(self, pipeline):
        post_data = {
            "name"     : pipeline.name,
            "version"  : pipeline.version
        }
        return self.post('/deactivate',json=post_data)
    
    def getExecutionStatus(self, exec_id):
        if exec_id is not None:
            try:
                return self.get("/execution/%s/status" % exec_id)
            except Exception as e:
                raise e
        
        raise RuntimeError("you must provide a valid execution_id")

    def getExecution(self, exec_id):
        if exec_id is not None:
            try:
                json_response = self.get("/execution/%s/get" % exec_id)
                if json_response["code"] == 202:
                    s_executor = dill.loads(base64.b64decode(json_response["executor"]["sobj"]))
                    executor = Execution.fromJson(s_executor)
                    return executor
                else:
                    return json_response

            except Exception as e:
                raise e

        
        raise RuntimeError("you must provide a valid execution_id")
     
    def getExecutionList(self, name):
        if name is not None or name!="":
            try:
                response = self.get("/execution/%s/list" % name)
                obj_list = []
                if response["code"]==204:
                    for sobj in response["executions"]:
                        obj = Execution.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("utf8"))))
                        obj_list.append(obj)
                    return obj_list
                elif response["code"]==311:
                    # no execution found
                    return []
                else:
                    raise APIResponseError("%d:%s" % (response["code"],response["status"]))
                
            except Exception as e:
                raise e
        
        raise RuntimeError("you must provide a valid execution_id")
    
    def getLastExecution(self, name):
        if name is not None:
            try:
                json_response = self.get("/execution/%s/last" % name)
                if json_response["code"] == 202:
                    s_executor = dill.loads(base64.b64decode(json_response["executor"]["sobj"]))
                    executor = Execution.fromJson(s_executor)
                    return executor
                else:
                    return json_response
                
            except Exception as e:
                raise e

    def createExecutor(self, pipeline, *args, **kwargs):
        executor = Executor(self, pipeline, *args, **kwargs)
        return executor

    def execute(self, pipeline, *args, **kwargs):
        post_data = {
            "name"     : pipeline.name,
            "version"  : pipeline.version,
            "args"     : jsonpickle.encode( args ),
            "kwargs"   : jsonpickle.encode( kwargs )
        }
        try:
            json_response = self.post("/execute",json=post_data)
            if json_response["code"] == 201:
                return json_response["execution_id"]
            else:
                raise PipelineExecutionError(json_response)
        except Exception as e:
            raise e
                
    def scheduleAt(self, pipeline, label = None, trigger_time=datetime.now().strftime("%H:%M:%S"), recurrency=None, tags=[]):
        post_data = {
            "name"         : pipeline.name,
            "label"        : label,
            "trigger_time" : trigger_time,
            "recurrency"   : recurrency,
            "tags"         : tags,
            "args"         : jsonpickle.encode( pipeline.getArguments() ),
            "kwargs"       : jsonpickle.encode( pipeline.getKeywordArguments() )
        }
        
        print(post_data)
        
        try:
            json_response = self.post("/scheduleAt",json=post_data)
            if json_response["code"] == 201:
                return json_response
            else:
                raise PipelineSchedulingError(json_response)
        except Exception as e:
            raise e

    def getScheduledExecutions(self, pipeline):
        if isinstance(pipeline, Pipeline):
            name = pipeline.name
            if name is not None or name!="":
                try:
                    response = self.get("/execution/%s/scheduled" % name)
                    obj_list = []
                    if response["code"]==204:
                        for sobj in response["scheduled"]:
                            obj = ScheduledEvent.fromJson(dill.loads(base64.b64decode(sobj["sobj"].encode("ascii"))))
                            obj_list.append(obj)
                        return obj_list
                    elif response["code"]==311:
                        # no execution found
                        return []
                    else:
                        raise APIResponseError("%d:%s" % (response["code"],response["status"]))

                except Exception as e:
                    raise e
        raise RuntimeError("pipeline argument must be a Pipeline instance")
    
    def getScheduledExecutionById(self, scheduled_event_id):
        if scheduled_event_id is not None and scheduled_event_id!="":
            try:
                response = self.get("/schedule/%s/get" % scheduled_event_id)
                if response["code"]==204:
                    return ScheduledEvent.fromJson(dill.loads(base64.b64decode(response["scheduled_evt"]["sobj"].encode("utf8"))))
                else:
                    raise APIResponseError("%d:%s" % (response["code"],response["status"]))

            except Exception as e:
                raise e
        else:
            raise RuntimeError("you must provide a valid scheduled event id")

    def cancelScheduledExecution(self, scheduled_event):
        if isinstance(scheduled_event,ScheduledEvent):
            try:
                response = self.get("/schedule/%s/cancel" % scheduled_event.uuid)
                if response["code"]==204:
                    return True
                else:
                    raise APIResponseError("%d:%s" % (response["code"],response["status"]))

            except Exception as e:
                raise e
        else:
            raise RuntimeError("you must provide a valid scheduled event")

    def putKey(self, key, passphrase=None):
        if not isinstance(key, EncryptionKey):
            raise RuntimeError("key argument must be a Key instance")
        if not isinstance(passphrase,(str,type(None))):
            raise RuntimeError("you must provide a valid passphrase")
        key.encodeKey()
        key_encode = jsonpickle.encode(key)
        key.decodeKey()
        post_data = {
            "label"      : key.getLabel(),
            "key"        : key_encode,
            "passphrase" : passphrase,
        }
        return self.post("/putKey",json=post_data)
        
    def getKey(self, label, passphrase=None):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(passphrase,(str,type(None))):
            raise RuntimeError("you must provide a valid passphrase")
        try:
            response = self.get("/key/%s/%s/getKey" %(label, passphrase))
            if response["code"]==205:
                key = EncryptionKey.deserialize(response['key'])
                response['key'] = key
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
            
    def getPublicKey(self): # NUEVA
        try:
            response = self.get("/key/getPublicKey")
            if response["code"]==205:
                key = EncryptionKey.deserialize(response['key'])
                response['key'] = key
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
        
    def getKeyList(self, active=True):
        if not isinstance(active,bool):
            raise RuntimeError("you must provide a valid argument")
        try:
            response = self.get("/KeyList/%s" % active)
            if response["code"]==206:
                keys = jsonpickle.loads(response['keys'])
                list_keys=[]
                for key in keys:
                    list_keys.append(EncryptionKey.deserialize(key))
                response['keys'] = list_keys
                return response
            elif response["code"]==312:
                #don't exist keys
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
    
    def keyExpiration(self, key):
        if not isinstance(key, EncryptionKey):
            raise RuntimeError("key argument must be a Key instance")
        key.encodeKey()
        key_encode = jsonpickle.encode(key)
        key.decodeKey()
        post_data = {
            "label"  : key.getLabel(),
            "key"    : key_encode,
        }
        try:
            response = self.post("/keyExpiration",json=post_data)
            if response["code"]==206 or response["code"]==314:
                response['key_status'] = jsonpickle.loads(response['key_status'])
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
    
    def getKeyExpirationDate(self, key):
        if not isinstance(key, EncryptionKey):
            raise RuntimeError("key argument must be a Key instance")
        
        key.encodeKey()
        key_encode = jsonpickle.encode(key)
        key.decodeKey()
        post_data = {
            "label"  : key.getLabel(),
            "key"    : key_encode,
        }
        try:
            response = self.post("/keyExpirationDate",json=post_data)
            if response["code"]==206:
                response['date'] = jsonpickle.loads(response['date'])
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
     
    def getCredentialList(self, active=True):
        if not isinstance(active,bool):
            raise RuntimeError("you must provide a valid argument")
        try:
            response = self.get("/credentialList/%s" % active)
            if response["code"]==206:
                response['credentials'] = jsonpickle.loads(response['credentials'])
                return response
            elif response["code"]==312:
                #don't exist credentials
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
     
    def credentialExpiration(self, label):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        try:
            response = self.get("/credential/%s/credentialExpiration" % label)
            if response["code"]==206 or response["code"]==314:
                response['credential_status'] = jsonpickle.loads(response['credential_status'])
                return response
            elif response["code"]==312 or response["code"]==313:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
            
    def getCredentialExpirationDate(self, label):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        try:
            response = self.get("/credential/%s/credentialExpirationDate" % label)
            if response["code"]==206:
                response['date'] = jsonpickle.loads(response['date'])
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
    
    def signCredential(self, credential, key=None):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")
        key_encode = None
        if key is not None:
            if not isinstance(key, EncryptionKey):
                raise RuntimeError("key argument must be a Key instance")
            key.encodeKey()
            key_encode= jsonpickle.encode(key)
            key.decodeKey()
        post_data = {
            "label"       : credential.getLabel(),
            "credential"  : jsonpickle.encode(credential),
            "key"         : key_encode,
        }
        
        return self.post("/signCredential",json=post_data)
    
    def putCredential(self, credential, n_unlock=2, shared_users=4):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")
        if not isinstance(n_unlock, int):
            raise RuntimeError("credential argument must be a Credential instance") 
        if not isinstance(shared_users, int):
            raise RuntimeError("credential argument must be a Credential instance") 
        post_data = {
            "label"         : credential.getLabel(),
            "credential"    : jsonpickle.encode(credential),
            "n_unlock"      : n_unlock,
            "shared_users"  : shared_users,
        }
        return self.post("/putCredential",json=post_data)
        
    def getCredential(self, label, decrypt=True, token=None, whom=None):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(decrypt,bool):
            raise RuntimeError("you must provide a valid decrypt argument")
        if not isinstance(whom,(str,type(None))):
            raise RuntimeError("you must provide a valid whom argument")
        token_encode = jsonpickle.encode(token)
        try:
            post_data = {
                "label"   : label,
                "decrypt" : decrypt,
                "token"   : token_encode,
                "whom"    : whom,
            }      
            response = self.post("/getCredential",json=post_data)
            if response["code"]==205:
                response['credential'] = Credential.deserialize(response['credential'])
                return  response
            elif response["code"]==312 or response["code"]==313:
                #don't exist credential or token not found to decrypt
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))   
        except Exception as e:
            raise e
    
    def verifyCredential(self, credential):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")  
        post_data = {
            "label"        : credential.getLabel(),
            "credential"  : jsonpickle.encode(credential),
        }      
        return self.post("/verifyCredential",json=post_data)
    
    def encryptCredential(self, credential, recipient_key):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")  
        if not isinstance(recipient_key, EncryptionKey):
                raise RuntimeError("key argument must be a Key instance")
        recipient_key.encodeKey()
        key_encode= jsonpickle.encode(recipient_key)
        recipient_key.decodeKey()
        post_data = {
            "credential" : jsonpickle.encode(credential),
            "key"        : key_encode,
        }
        try:
            response = self.post("/encryptCredential",json=post_data)
            if response["code"]==206:
                response['credential_encrypted'] = jsonpickle.loads(response['credential_encrypted'])
                return response 
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e        
        
    def createToken(self, credential, min_unlock=2, shared_users=4):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")
        if not isinstance(min_unlock, int):
            raise RuntimeError("argument must be int")
        if not isinstance(shared_users, int):
            raise RuntimeError("argument must be int")
        post_data = {
            "credential"     : jsonpickle.encode(credential),
            "minimum_unlock" : min_unlock,
            "shared_users"   : shared_users, 
        }
        try:
            response = self.post("/createToken",json=post_data)
            if response["code"]==206:
                response["token"] = jsonpickle.loads(response["token"])
                response["credential_encrypted"] = jsonpickle.loads(response["credential_encrypted"])
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e

    def putToken(self, credential, token_list):
        if not isinstance(credential, Credential):
            raise RuntimeError("credential argument must be a Credential instance")
        if not isinstance(token_list, list):
            raise RuntimeError("argument must be a list")
        post_data = {
            "credential"  : jsonpickle.encode(credential),
            "token_list"  : jsonpickle.encode(token_list),
        }        
        return self.post("/putToken",json=post_data)
    
    def getToken(self, label=None, whom=None, active=False):
        if not isinstance(label,(str,type(None))):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(whom,(str,type(None))):
            raise RuntimeError("you must provide a valid whom argument")
        try:
            response = self.get("/credential/%s/%s/%s/getToken" % (label, whom, active))
            if response["code"]==205:
                response['token'] = jsonpickle.loads(response['token'])
                return response 
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
    
    def assignToken(self, whom, label, exp_date, comment=""):
        if not isinstance(whom,str):
            raise RuntimeError("you must provide a valid whom argument")
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(comment,str):
            raise RuntimeError("you must provide a valid comment argument")
        post_data = {
            "whom"      : whom,
            "label"     : label,
            "exp_date"  : jsonpickle.encode(exp_date),
            "comment"   : comment,
        }
        
        return self.post("/assignToken",json=post_data)
    
    def getAssignedToken(self, label, whom):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(whom,str):
            raise RuntimeError("you must provide a valid whom argument")
        try:
            response = self.get("/token/%s/%s/getAssignedToken" %(label, whom))
            if response["code"]==205:
                response['token'] = jsonpickle.loads(response['token'])
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
    
    def signToken(self, label, whom):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(whom,str):
            raise RuntimeError("you must provide a valid whom argument")
        post_data = {
            "label" : label,
            "whom"  : whom,
        }
        return self.post("/signToken",json=post_data)
        
    def verifyToken(self, token):  
        post_data = {
            "token"  : jsonpickle.encode(token),
        }      
        return self.post("/verifyToken",json=post_data)
    
    def getTokenList(self, active=False):
        if not isinstance(active,bool):
            raise RuntimeError("you must provide a valid argument")
        try:
            response = self.get("/tokenList/%s" % active)
            if response["code"]==206:
                response['token_list'] = jsonpickle.loads(response['token_list'])
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
    
    def getTokenExpirationDate(self, label, whom):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(whom,str):
            raise RuntimeError("you must provide a valid label")
        try:
            response = self.get("/token/%s/%s/tokenExpirationDate" %(label, whom))
            if response["code"]==206:
                response['date'] = jsonpickle.loads(response['date'])
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
    
    def tokenExpiration(self, label, whom=None):
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(whom,(str,type(None))):
            raise RuntimeError("you must provide a valid label")
        try:
            response = self.get("/token/%s/%s/tokenExpiration" % (label, whom))
            if response["code"]==206:
                response['token_status'] = jsonpickle.loads(response['token_status'])
                return response
            elif response["code"]==312 or response["code"]==313:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))
        except Exception as e:
            raise e
            
    def getCredential(self, label, token=None, whom=None): # NUEVA
        if not isinstance(label,str):
            raise RuntimeError("you must provide a valid label")
        if not isinstance(token,(str,type(None))):
            raise RuntimeError("you must provide a valid token")
        if not isinstance(whom,(str,type(None))):
            raise RuntimeError("you must provide a valid argument")
        try:
            response = self.get("/credential/%s/%s/%s/getRegisterCredential" % (label, token, whom))
            if response["code"]==205:
                #credential = Credential.deserialize(response['credential'])
                credential = jsonpickle.loads(response['credential'])
                response['credential'] = credential
                return response
            elif response["code"]==312:
                return response
            else:
                raise APIResponseError("%d:%s" % (response["code"],response["status"]))

        except Exception as e:
            raise e
