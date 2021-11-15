from flask import request, jsonify
from multiprocessing import Process, Queue, Manager
from datetime import date, datetime, timezone
from tzlocal import get_localzone
import jsonpickle
import inspect
import time
import dill
import base64
import ast
from datetime import datetime

from ..base import AbstractApiService
from ..loggers import BasicLogger
from ..scheduler import ScheduledEvent
from ..exceptions import MultipleActivePipelineRegistered, NoActivePipelineRegistered,PipelineExecutionError
from .OrchestratorManager import OrchestratorManager
from .OrchestratorAccess import OrchestratorAccess

from bupacl.credentialmanager.exceptions import *

class OrchestratorService(AbstractApiService):
    def __init__(self,address="127.0.0.1", port=8020, db_conn_str="sqlite:///orchestrator.sqlite"):
        super().__init__("OrchestratorService", bind_addr=address, bind_port=port)
        self.addRule("/","status",self.status)
        self.addRule("/stop","stop",self.stop)
        self.addRule("/register","register",self.register, methods=["POST"])
        self.addRule("/get","get",self.get, methods=["POST"])
        self.addRule("/activate","activate",self.activate, methods=["POST"])
        self.addRule("/deactivate","deactivate",self.deactivate, methods=["POST"])
        self.addRule("/execute","execute",self.execute, methods=["POST"])        
        self.addRule("/execution/<exec_id>/status","execution_status",self.execution_status, methods=["GET"])
        self.addRule("/execution/<exec_id>/get","get_execution",self.get_execution, methods=["GET"])
        self.addRule("/execution/<pipeline_name>/list","get_execution_list",self.get_execution_list, methods=["GET"])
        self.addRule("/execution/<pipeline_name>/last","get_last_execution",self.get_last_execution, methods=["GET"])
        self.addRule("/execution/<pipeline_name>/scheduled","get_scheduled_executions",self.get_scheduled_executions, methods=["GET"])
        self.addRule("/schedule/<scheduled_execution_id>/cancel","cancel_scheduled_execution",self.cancel_scheduled_execution, methods=["GET"])
        self.addRule("/schedule/<scheduled_execution_id>/get","get_scheduled_execution_by_id",self.get_scheduled_execution_by_id, methods=["GET"])
        self.addRule("/scheduleAt","scheduleAt",self.scheduleAt, methods=["POST"])
        self.addRule("/rpn/<label>/notify","create_notification",self.create_notification, methods=["GET"])
        self.addRule("/rpn/<label>/last","get_last_notification",self.get_last_notification, methods=["GET"])
        self.addRule("/rpn/<label>/list","get_notification_list",self.get_notification_list, methods=["GET"])
        self.addRule("/putKey","put_key",self.put_key, methods=["POST"])
        self.addRule("/putCredential","put_credential",self.put_credential, methods=["POST"])
        self.addRule("/putToken","put_token",self.put_token, methods=["POST"])
        self.addRule("/signCredential","sign_credential",self.sign_credential, methods=["POST"])
        self.addRule("/verifyCredential","verify_credential",self.verify_credential, methods=["POST"])
        self.addRule("/keyExpiration","key_expiration",self.key_expiration, methods=["POST"])
        self.addRule("/keyExpirationDate","get_key_expiration_date",self.get_key_expiration_date, methods=["POST"])
        self.addRule("/encryptCredential","encrypt_credential",self.encrypt_credential, methods=["POST"])
        self.addRule("/createToken","create_token",self.create_token, methods=["POST"])
        self.addRule("/key/<label>/<passphrase>/getKey","get_key",self.get_key, methods=["GET"])
        self.addRule("/key/getPublicKey","get_public_key",self.get_public_key, methods=["GET"])
        self.addRule("/KeyList/<active>","get_key_list",self.get_key_list, methods=["GET"])
        self.addRule("/credentialList/<active>","get_credential_list",self.get_credential_list, methods=["GET"])
        self.addRule("/getCredential","get_credential",self.get_credential, methods=["POST"])
        self.addRule("/credential/<label>/<whom>/<active>/getToken","get_token",self.get_token, methods=["GET"])
        self.addRule("/credential/<label>/credentialExpiration","credential_expiration",self.credential_expiration, methods=["GET"])
        self.addRule("/credential/<label>/credentialExpirationDate","get_credential_expiration_date",self.get_credential_expiration_date, methods=["GET"])
        self.addRule("/credential/<label>/<token>/<whom>/getRegisterCredential","get_register_credential",self.get_register_credential, methods=["GET"])
        self.addRule("/assignToken","assign_token",self.assign_token, methods=["POST"])
        self.addRule("/token/<label>/<whom>/getAssignedToken","get_assigned_token",self.get_assigned_token, methods=["GET"])
        self.addRule("/signToken","sign_token",self.sign_token, methods=["POST"])
        self.addRule("/verifyToken","verify_token",self.verify_token, methods=["POST"])
        self.addRule("/tokenList/<active>","get_token_list",self.get_token_list, methods=["GET"])
        self.addRule("/token/<label>/<whom>/tokenExpirationDate","get_token_expiration_date",self.get_token_expiration_date, methods=["GET"])
        self.addRule("/token/<label>/<whom>/tokenExpiration","token_expiration",self.token_expiration, methods=["GET"])
        
        self.manager = OrchestratorManager(db_conn_str=db_conn_str)
        
        self.setLogger(BasicLogger("OrchestratorService"))
        
    def status(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        return {
            "code"   : 200,
            "status" : "running",
            "ts"     : now.isoformat()
        }
    
    def register(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json
        
        name         = json_input["name"]
        pipeline_fn  = dill.loads(base64.b64decode(json_input["pipeline"].encode("utf8")))
        new_version  = json_input["new_version"]
        token_list   = ast.literal_eval(json_input["token_list"])

        if not new_version:
            if not self.manager.isPipelineRegistered(name):
                self.logger.info("registering first version for %s" % name)
                try:
                    #TODO: handle the owner in the api
                    new_pipeline = self.manager.register(name,pipeline_fn,token_list)

                    return {
                        "code"   : 200,
                        "status" : "registered",
                        "ts"     : now.isoformat()
                    }
                except Exception as e:
                    self.logger.info("error registering %s: %s" % (name,e))
                    return {
                        "code"   : 302,
                        "status" : "Could not register pipeline",
                        "error"  : "%s" % e,
                        "ts"     : now.isoformat()
                    }

            self.logger.info("already registered %s" % name)
            return {
                "code"   : 301,
                "status" : "Already Registered",
                "ts"     : now.isoformat()
            }
        else:
            if self.manager.isPipelineRegistered(name):
                self.logger.info("registering new version for %s" % name)
                try:
                    #TODO: handle the owner in the api
                    new_pipeline = self.manager.register(name,pipeline_fn,token_list,new_version = True)

                    return {
                        "code"   : 200,
                        "status" : "new version registered: %f" % new_pipeline.version,
                        "ts"     : now.isoformat()
                    }
                except Exception as e:
                    self.logger.info("error registering %s: %s" % (name,e))
                    return {
                        "code"   : 302,
                        "status" : "Could not register pipeline",
                        "error"  : "%s" % e,
                        "ts"     : now.isoformat()
                    }

            self.logger.info("no previous versions for pipeline %s found" % name)
            return {
                "code"   : 301,
                "status" : "No previous version found for pipeline %s" % name,
                "ts"     : now.isoformat()
            }

    def get(self): 
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json
                
        name        = json_input["name"]
        
        if self.manager.isPipelineRegistered(name):
            self.logger.info("getting %s" % name)
            try:
                pipelines = self.manager.getPipelines(**json_input)
                return {
                    "code"     : 200,
                    "status"   : "ok",
                    "pipeline" : [p.asJson() for p in pipelines],
                    "ts"       : now.isoformat()
                }
            except Exception as e:
                self.logger.info("error getting %s: %s" % (name,e))
                return {
                    "code"   : 302,
                    "status" : "%s" % e,
                    "ts"     : now.isoformat()
                }
        
        self.logger.info("pipeline not registered %s" % name)
        return {
            "code"   : 301,
            "status" : "Pipeline Not Registered",
            "ts"     : now.isoformat()
        }
    
    def activate(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json

        name        = json_input["name"]
        
        if self.manager.isPipelineRegistered(name):
            self.logger.info("getting %s" % name)
            try:
                pipelines = self.manager.getPipelines(**json_input)
                
                if len(pipelines)==1:
                    pipeline = pipelines[0]
                    
                    if pipeline.isActive():
                        return {
                            "code"     : 203,
                            "status"   : "already active",
                            "ts"       : now.isoformat()
                        }
                    
                    # deactivate all the pipelines with given name
                    self.manager.deactivateAll(pipeline)
                    
                    # activate the pipeline
                    if pipeline.setActive(True):
                        return {
                            "code"     : 202,
                            "status"   : "active",
                            "ts"       : now.isoformat()
                        }
                    else:
                        return {
                            "code"   : 306,
                            "status" : "could not activate pipeline %s" % name,
                            "ts"     : now.isoformat()
                        }
                        
                elif len(pipelines)>1:
                    # this should never happend (multiple active pipelines)
                    e = MultiplePipelineFound(name)
                    return {
                        "code"   : 305,
                        "status" : "%s:%s" % (type(e).__name__,e),
                        "ts"     : now.isoformat()
                    }
                else:
                    return {
                        "code"   : 307,
                        "status" : "pipeline not found or already active:" % name,
                        "ts"     : now.isoformat()
                    }
            except Exception as e:            
                self.logger.info("error getting %s: %s" % (name,e))
                return {
                    "code"   : 305,
                    "status" : "%s:%s" % (type(e).__name__,e),
                    "ts"     : now.isoformat()
                }
        
        self.logger.info("pipeline not registered %s" % label)
        return {
            "code"   : 301,
            "status" : "Pipeline Not Registered",
            "ts"     : now.isoformat()
        }

    def deactivate(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json
                
        name        = json_input["name"]
        
        if self.manager.isPipelineRegistered(name):
            self.logger.info("getting %s" % name)
            try:
                pipelines = self.manager.getPipelines(**json_input, active = True)
                
                if len(pipelines)==1:
                    pipeline = pipelines[0]
                    
                    if not pipeline.isActive():
                        return {
                            "code"     : 203,
                            "status"   : "already not active",
                            "ts"       : now.isoformat()
                        }
                    # deactivate the pipeline
                    if pipeline.setActive(False):
                        return {
                            "code"     : 202,
                            "status"   : "not active",
                            "ts"       : now.isoformat()
                        }
                    else:
                        return {
                            "code"   : 308,
                            "status" : "could not deactivate pipeline %s" % name,
                            "ts"     : now.isoformat()
                        }
                        
                elif len(pipelines)>1:
                    e = MultipleActivePipelineRegistered(name)
                    return {
                        "code"   : 305,
                        "status" : "%s:%s" % (type(e).__name__,e),
                        "ts"     : now.isoformat()
                    }
                else:
                    return {
                        "code"   : 307,
                        "status" : "pipeline not found or already deactivated:" % name,
                        "ts"     : now.isoformat()
                    }
            except Exception as e:
                self.logger.info("error getting %s: %s" % (name,e))
                return {
                    "code"   : 305,
                    "status" : "%s:%s" % (type(e).__name__,e),
                    "ts"     : now.isoformat()
                }
        
        self.logger.info("pipeline not registered %s" % label)
        return {
            "code"   : 301,
            "status" : "Pipeline Not Registered",
            "ts"     : now.isoformat()
        }
    
    def execution_status(self, exec_id):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        print("%s : getting status for execution id %s" % (now, exec_id))
        
        executor = self.manager.getExecution(exec_id)
        
        if executor is not None:
            return {
                "code"         : 202,
                "status"       : "executor active",
                "state"        : executor.state,
                "execution_id" : exec_id,
                "ts"           : now.isoformat()
            }
        else:
            return {
                "code"         : 310,
                "status"       : "Executor gone",
                "state"        : "unknown",
                "execution_id" : exec_id,
                "ts"           : now.isoformat()
            }
    
    def get_execution(self, exec_id):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        print("%s : getting execution id %s" % (now, exec_id))
        
        executor = self.manager.getExecution(exec_id)
        
        if executor is not None:
            return {
                "code"         : 202,
                "status"       : "ok",
                "executor"     : executor.serialize(),
                "execution_id" : exec_id,
                "ts"           : now.isoformat()
            }
        else:
            return {
                "code"         : 310,
                "status"       : "Executor does not exists",
                "executor"     : "",
                "execution_id" : exec_id,
                "ts"           : now.isoformat()
            }
    
    def execute(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json
        
        pipeline_args   = []
        pipeline_kwargs = {}
        
        name            = json_input["name"]
        version         = json_input["version"]

        local_job       = True
        cores           = 1
        partition       = None
        memory          = None

        if "asJob" in json_input:
            local_job = json_input["asJob"]

        if "cores" in json_input:
            cores = int(json_input["cores"])

        if "partition" in json_input:
            partition = int(json_input["partition"])

        if "memory" in json_input:
            memory = int(json_input["memory"])

        if "args" in json_input:
            pipeline_args   = jsonpickle.loads(json_input["args"])
        
        if "kwargs" in json_input:
            pipeline_kwargs = jsonpickle.loads(json_input["kwargs"])

        pipelines = []
        try:        
            pipelines = self.manager.getPipelines(name = name, version = version)
            
        except Exception as e:
            return {
                "code"        : 305,
                "status"      : "%s : %s" % (type(e).__name__, e),
                "ts"          : now.isoformat()
            }
        
        if len(pipelines)==1:
            pipeline = pipelines[0]
            try:
                executor = None
                if local_job:
                    executor = self.manager.execute(pipeline, *pipeline_args,**pipeline_kwargs)
                else:
                    executor = self.manager.createExecutor(pipeline, local = local_job, cores=cores, partition=partition, memory=memory)
                    executor.run(*pipeline_args,**pipeline_kwargs)

                return {
                    "code"         : 201,
                    "status"       : executor.state,
                    "execution_id" : executor.getExecutionId(),
                    "ts"           : now.isoformat()
                }
            except Exception as e:
                self.logger.info("execution failed:",e)
                return {
                    "code"        : 303,
                    "status"      : "Execution failed. %s" % e,
                    "ts"          : now.isoformat()
                }
        elif len(pipelines)>1:
            e = MultipleActivePipelineRegistered(name)
            return {
                "code"   : 305,
                "status" : "%s:%s" % (type(e).__name__,e),
                "ts"     : now.isoformat()
            }
        else:
            e = NoActivePipelineRegistered(name)
            return {
                "code"   : 306,
                "status" : "%s:%s" % (type(e).__name__,e),
                "ts"     : now.isoformat()
            }

    def scheduleAt(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        json_input = request.json

        name            = json_input["name"]
        label           = json_input["label"]
        trigger_time    = json_input["trigger_time"]
        recurrency      = json_input["recurrency"]
        tags            = json_input["tags"]
        pipeline_args   = jsonpickle.loads(json_input["args"])
        pipeline_kwargs = jsonpickle.loads(json_input["kwargs"])

        pipelines = []
        try:        
            pipelines = self.manager.getPipelines(name = name, active = True)
            
        except Exception as e:
            return {
                "code"        : 305,
                "status"      : "%s : %s" % (type(e).__name__, e),
                "ts"          : now.isoformat()
            }
        
        if len(pipelines)==1:
            pipeline = pipelines[0]
            pipeline.setArguments(pipeline_args)
            pipeline.setKeywordArguments(pipeline_kwargs)
            try:
                
                if self.manager.scheduleAt(pipeline, trigger_time=trigger_time, label=label, recurrency=recurrency,tags=tags):
                    return {
                        "code"         : 201,
                        "status"       : True,
                        "ts"           : now.isoformat()
                    }
            except Exception as e:
                self.logger.info("scheduling failed:",e)
                return {
                    "code"        : 303,
                    "status"      : "Scheduling failed. %s" % e,
                    "ts"          : now.isoformat()
                }
        elif len(pipelines)>1:
            e = MultipleActivePipelineRegistered(name)
            return {
                "code"   : 305,
                "status" : "%s:%s" % (type(e).__name__,e),
                "ts"     : now.isoformat()
            }
        else:
            e = NoActivePipelineRegistered(name)
            return {
                "code"   : 306,
                "status" : "%s:%s" % (type(e).__name__,e),
                "ts"     : now.isoformat()
            }
    
    def get_last_execution(self, pipeline_name):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        print("%s : getting last execution for %s" % (now, pipeline_name))
        
        executor = self.manager.getLastExecution(pipeline_name)
        
        if executor is not None:
            return {
                "code"         : 202,
                "status"       : "ok",
                "executor"     : executor.serialize(),
                "ts"           : now.isoformat()
            }
        else:
            return {
                "code"         : 310,
                "status"       : "Executor does not exists",
                "executor"     : None,
                "ts"           : now.isoformat()
            }
    
    def get_execution_list(self,pipeline_name):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        
        if self.manager.isPipelineRegistered(pipeline_name):
            self.logger.info("getting execution list for %s" % pipeline_name)
            
            executions = self.manager.getExecutionList(pipeline_name)
            
            if len(executions)>0:
                executors_list_json = [ ex.serialize() for ex in executions ]
                
                return {
                    "code"         : 204,
                    "status"       : "ok",
                    "pipeline_name": pipeline_name,
                    "executions"   : executors_list_json,
                    "ts"           : now.isoformat()
                }
            else:
                return {
                    "code"         : 311,
                    "status"       : "No executions found",
                    "pipeline_name": pipeline_name,
                    "executions"   : [],
                    "ts"           : now.isoformat()
                }

        self.logger.info("pipeline not registered %s" % label)
        return {
            "code"         : 301,
            "status"       : "Pipeline Not Registered",
            "pipeline_name": pipeline_name,
            "ts"           : now.isoformat()
        }

    def get_scheduled_executions(self,pipeline_name):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        
        if self.manager.isPipelineRegistered(pipeline_name):
            self.logger.info("getting scheduled executions for %s" % pipeline_name)
            
            pipelines = []
            try:        
                pipelines = self.manager.getPipelines(name = pipeline_name, active = True)

            except Exception as e:
                return {
                    "code"        : 305,
                    "status"      : "%s : %s" % (type(e).__name__, e),
                    "ts"          : now.isoformat()
                }
        
            if len(pipelines)==1:
                pipeline = pipelines[0]

                sch_execs = self.manager.getScheduledExecutions(pipeline)
            
                if len(sch_execs)>0:
                    sch_execs_list_json = [ x.serialize() for x in sch_execs ]

                    return {
                        "code"         : 204,
                        "status"       : "ok",
                        "pipeline_name": pipeline_name,
                        "scheduled"    : sch_execs_list_json,
                        "ts"           : now.isoformat()
                    }
                else:
                    return {
                        "code"         : 311,
                        "status"       : "No scheduled executions found",
                        "pipeline_name": pipeline_name,
                        "scheduled"    : [],
                        "ts"           : now.isoformat()
                    }
            else:
                self.logger.info("multiple active pipelines registered %s" % pipeline_name)
                return {
                    "code"         : 303,
                    "status"       : "Multiple active pipelines registered",
                    "pipeline_name": pipeline_name,
                    "ts"           : now.isoformat()
                }
            
        self.logger.info("pipeline not registered %s" % pipeline_name)
        return {
            "code"         : 301,
            "status"       : "Pipeline Not Registered",
            "pipeline_name": pipeline_name,
            "ts"           : now.isoformat()
        }
    
    def get_scheduled_execution_by_id(self,scheduled_execution_id):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        
        if (scheduled_execution_id is not None and scheduled_execution_id!=""):
            sch_evt = self.manager.getScheduledExecutionById(scheduled_execution_id)

            if isinstance(sch_evt,ScheduledEvent):
                sch_exec_json = sch_evt.serialize()

                return {
                    "code"          : 204,
                    "status"        : "ok",
                    "scheduled_evt" : sch_exec_json,
                    "ts"            : now.isoformat()
                }
            else:
                return {
                    "code"          : 311,
                    "status"        : "No scheduled execution found",
                    "scheduled_evt" : None,
                    "ts"            : now.isoformat()
                }
        else:
            raise RuntimeError("you must provide a valid scheduled event id")
    
    def isPipelineRegistered(self, pipeline_name):
        return self.manager.isPipelineRegistered(pipeline_name)

    def cancel_scheduled_execution(self, scheduled_execution_id):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        
        sch_evt = self.manager.getScheduledExecutionById(scheduled_execution_id)
    
        if isinstance(sch_evt, ScheduledEvent):    
            if self.manager.cancelScheduledExecution(sch_evt):
                return {
                    "code"         : 204,
                    "status"       : "ok",
                    "ts"           : now.isoformat()
                }
            else:
                return {
                    "code"          : 311,
                    "status"        : "scheduled event not cancelled",
                    "scheduled_evt" : None,
                    "ts"            : now.isoformat()
                }

    def create_notification(self,label):
        
        data = request.args
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        notification = self.manager.createNotification(label,data=data)
        
        if notification is not None:
            notif_json = notification.serialize()
            return {
                "code"         : 204,
                "status"       : "ok",
                "notification" : notif_json,
                "ts"           : now.isoformat()
            }
        else:
            return {
                "code"          : 311,
                "status"        : "notification not created",
                "notification"  : None,
                "ts"            : now.isoformat()
            }
        
    def get_last_notification(self,label):
        now = datetime.now(timezone.utc).astimezone(get_localzone())        
        last_notification = self.manager.getLastNotification(label)
        
        if last_notification is not None:
            notif_json = last_notification.serialize()
            return {
                "code"         : 204,
                "status"       : "ok",
                "notification" : notif_json,
                "ts"           : now.isoformat()
            }
        else:
            return {
                "code"          : 311,
                "status"        : "last notification not found",
                "notification"  : None,
                "ts"            : now.isoformat()
            }
        
    def get_notification_list(self,label):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        notification_list = self.manager.getNotificationList(label)
        if len(notification_list)>0:
            notif_list_json = []
            for n in notification_list:
                notif_list_json.append(n.serialize())
            
            return {
                "code"              : 204,
                "status"            : "ok",
                "notification_list" : notif_list_json,
                "ts"                : now.isoformat()
            }
        else:
            return {
                "code"              : 311,
                "status"            : "last notification not found",
                "notification_list" : [],
                "ts"                : now.isoformat()
            }
    
    def stop(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        self.manager.stop()
        super().release()
        return {
            "code"   : 210,
            "status" : "stop",
            "ts"     : now.isoformat()
        }
    
    def put_key(self):
        json_input = request.json
        label = json_input["label"]
        passphrase = json_input["passphrase"]
        key_json = json_input['key']
        key = jsonpickle.loads(key_json)
        key.decodeKey()
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            self.logger.info("storing key with label:%s" % (label))
            self.manager.putKey(key, passphrase)
            return {
                "code"   : 205,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except AlreadyExists as e:
            self.logger.info("%s" % (e))
            return {
                "code"   : 312,
                "status" : "Could not store key",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error storing key with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not store key",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }            
        
    def get_key(self, label, passphrase):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            key = self.manager.getKey(label, passphrase)
            key_encode = key.serialize()
            return {
                "code"   : 205,
                "status"   : "ok",
                "key"    : key_encode,
                "ts"     : now.isoformat()
            }
        except KeyNotFound as e:
            self.logger.info("%s" % (e))
            return {
                "code"   : 312,
                "status" : "key not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error retrieving  key with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve key",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
    def get_public_key(self):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            key = self.manager.getPublicKey()
            key_encode = key.serialize()
            return {
                "code"   : 205,
                "status" : "ok",
                "key"    : key_encode,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error retrieving public key :%s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve key",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
            
            
    def put_credential(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        label = json_input["label"]
        credential_json = json_input['credential']
        n_unlock = json_input["n_unlock"]
        shared_users = json_input["shared_users"]
        credential = jsonpickle.loads(credential_json)
        try:
            self.logger.info("storing credential with label:%s" % (label))
            self.manager.putCredential(credential, n_unlock, shared_users)
            return {
                "code"   : 205,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error storing credential with label %s: %s" % (label,e))
            return {
                "code"   : 312,
                "status" : "Could not store credential",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
    def get_credential(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        label = json_input['label']
        decrypt = json_input["decrypt"]
        token_json = json_input["token"]
        token = jsonpickle.loads(token_json)
        whom = json_input["whom"]
        try:
            credential = self.manager.getCredential(label, decrypt, token, whom)
            return {
                "code"       : 205,
                "status"     : "ok",
                "credential" : credential.serialize(),
                "ts"         : now.isoformat()
            }
        except CredentialNotFound as e:
            return {
                    "code"   : 312,
                    "status" : "credential not found",
                    "error"  : "%s" % e,
                    "ts"     : now.isoformat()
                }
        except TokenNotFound as e:
            return {
                    "code"   : 313,
                    "status" : "Token not Found",
                    "error"  : "%s" % e,
                    "ts"     : now.isoformat()
                }
        except Exception as e:
                self.logger.info("error retrieving  credential with label %s: %s" % (label,e))
                return {
                    "code"   : 314,
                    "status" : "Could not retrieve credential",
                    "error"  : "%s" % e,
                    "ts"     : now.isoformat()
                }
        
    def get_key_list(self, active):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            active = ast.literal_eval(active)
            status = 'inactive'
            if active is True:
                status = 'active'
            list_keys = self.manager.getKeyList(active)
            if len(list_keys) != 0:
                keys = []
                for key in list_keys:
                    keys.append(key.serialize())
                return {
                    "code"   : 206,
                    "status" : "ok",
                    "keys"   : jsonpickle.encode(keys),
                    "ts"     : now.isoformat()
                }
            else:
                self.logger.info("no keys %s registered in the vault"%(status))
                return {
                    "code"   : 312,
                    "status" : "there are no keys",
                    "ts"     : now.isoformat()
                }
        except Exception as e:
            self.logger.info("error listing keys: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not list keys",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
    
    def key_expiration(self):
        json_input = request.json
        label = json_input["label"]
        key_json = json_input['key']
        key = jsonpickle.loads(key_json)
        key.decodeKey()
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            status = self.manager.keyExpiration(key)
            return {
                "code"       : 206,
                "status"     : "ok",
                "key_status" : jsonpickle.encode(status),
                "ts"         : now.isoformat()
            }
        except KeyNotFound as e:
            return {
                "code"   : 312,
                "status" : "key not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except ExpiredKey as e:
            return {
                "code"   : 314,
                "status" : "key expired",
                'key_status': jsonpickle.encode(False),
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error verifying expiration of key with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not verify expiration of key",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
    
    def get_key_expiration_date(self):
        json_input = request.json
        label = json_input["label"]
        key_json = json_input['key']
        key = jsonpickle.loads(key_json)
        key.decodeKey()
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            date = self.manager.getKeyExpirationDate(key)
            return {
                "code"   : 206,
                "status" : "ok",
                "date"   : jsonpickle.encode(date),
                "ts"     : now.isoformat()
            }
        except KeyNotFound as e:
            return {
                "code"   : 312,
                "status" : "key not found",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error geting expiration date of key with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not get key expiration date",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
            
    
    def get_credential_list(self, active):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            active = ast.literal_eval(active)
            status = 'inactive'
            if active is True:
                status = 'active'
            list_credentials = self.manager.getCredentialList(active)
            if len(list_credentials) != 0:
                return {
                    "code"        : 206,
                    "status"      : "ok",
                    "credentials" : jsonpickle.encode(list_credentials),
                    "ts"          : now.isoformat()
                }
            else:
                self.logger.info("no credentials %s registered in the vault"%(status))
                return {
                    "code"        : 312,
                    "status"      : "there are no credentials",
                    "ts"          : now.isoformat()
                }
        except Exception as e:
            self.logger.info("error listing credentials: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not list credentials",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }    
        
                
    def credential_expiration(self, label):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            status = self.manager.credentialExpiration(label)
            return {
                "code"              : 206,
                "status"            : "ok",
                "credential_status" : jsonpickle.encode(status),
                "ts"                : now.isoformat()
            }
        except CredentialNotFound as e:
            return {
                "code"   : 312,
                "status" : "credential not found",
                "ts"     : now.isoformat()
            }
        except ExpiredCredential as e:
            return {
                "code"   : 314,
                "status" : "credential expired",
                "credential_status" : jsonpickle.encode(False),
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error checking credential: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not check credentials",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
                
    def get_credential_expiration_date(self, label):       
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            date = self.manager.getCredentialExpirationDate(label)
            return {
                "code"   : 206,
                "status" : "ok",
                "date"   : jsonpickle.encode(date),
                "ts"     : now.isoformat()
            }
        except CredentialNotFound as e:
            return {
                "code"   : 312,
                "status" : "credential not found",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error listing credentials: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not list credentials",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
    def sign_credential(self):
        json_input = request.json
        credential_json = json_input['credential']
        credential = jsonpickle.loads(credential_json)
        key_json = json_input['key']
        key = json_input['key']
        if key_json is not None:
            key = jsonpickle.loads(key_json)
            key.decodeKey()
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        label= credential.getLabel()
        try:
            self.logger.info("signing credential with label:%s" % (label))
            self.manager.signCredential(credential, key)
            return {
                "code"   : 206,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error signing credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not sign credential",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
            
    def verify_credential(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        label = json_input["label"]
        credential_json = json_input['credential']
        credential = jsonpickle.loads(credential_json)
        try:
            self.logger.info("checking sign of credential with label:%s" % (label))
            self.manager.verifyCredential(credential)
            return {
                "code"   : 206,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error checking credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not check credential",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
    def encrypt_credential(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        key_json = json_input["key"]
        key = jsonpickle.loads(key_json)
        key.decodeKey()
        credential_json = json_input['credential']
        credential = jsonpickle.loads(credential_json)
        label = credential.getLabel()
        try:
            self.logger.info("encrypt credential with label:%s" % (label))
            credential_encrypted = self.manager.encryptCredential(credential, key)
            return {
                "code"   : 206,
                "status" : "ok",
                "credential_encrypted" : jsonpickle.encode(credential_encrypted),
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error encrypting credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not encrypt credential",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }        
        
    def create_token(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        credential_json = json_input['credential']
        credential = jsonpickle.loads(credential_json)
        label = credential.getLabel()
        min_unlock = json_input['minimum_unlock']
        shared_users = json_input['shared_users']
        try:
            self.logger.info("creating token for credential with label:%s" % (label))
            token = self.manager.createToken(credential, min_unlock, shared_users)
            return {
                "code"   : 206,
                "status" : "ok",
                "token"  : jsonpickle.encode(token),
                "credential_encrypted" : jsonpickle.encode(credential),
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error creating token for credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not create token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
    def put_token(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        credential_json = json_input['credential']
        credential = jsonpickle.loads(credential_json)
        label = credential.getLabel()
        token_json = json_input['token_list']
        token_list = jsonpickle.loads(token_json)
        try:
            self.logger.info("storing token for credential with label:%s" % (label))
            self.manager.putToken(credential, token_list)
            return {
                "code"   : 205,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error storing token for credential with label %s: %s" % (label,e))
            return {
                "code"   : 312,
                "status" : "Could not store token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }        
        
    def get_token(self, label, whom, active):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        status = ast.literal_eval(active)
        try:
            token = self.manager.getToken(label, whom, status)
            return {
                "code"   : 205,
                "status" : "ok",
                "token"  : jsonpickle.encode(token),
                "ts"     : now.isoformat()
            }
        except TokenNotFound as e:
            return {
                    "code"   : 312,
                    "status" : "token not found",
                    "error"  : "%s" % e,
                    "ts"     : now.isoformat()
                }
        except Exception as e:
            self.logger.info("error retrieving token for credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        
        
    def assign_token(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())        
        whom = json_input['whom']
        label = json_input['label']
        exp_date_json = json_input['exp_date']
        exp_date = jsonpickle.loads(exp_date_json)
        comment = json_input['comment']
        try:
            assigned_token = self.manager.assignToken(whom, label, exp_date, comment)
            return {
                "code"   : 205,
                "status" : "ok",
                "token"  : jsonpickle.encode(assigned_token),
                "ts"     : now.isoformat()
            }
        except TokenNotFound as e:
            return {
                "code"   : 312,
                "status" : "token not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error assigning token for label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }


    def get_assigned_token(self, label, whom):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            token = self.manager.getAssignedToken(label, whom)
            return {
                "code"   : 205,
                "status" : "ok",
                "token"  : jsonpickle.encode(token),
                "ts"     : now.isoformat()
            }
        except TokenNotFound as e:
            return {
                "code"   : 312,
                "status" : "token not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error retrieving token for label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }


    def sign_token(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())        
        label = json_input['label']
        whom = json_input['whom']
        try:
            self.logger.info("signing token with label:%s" % (label))
            self.manager.signToken(label, whom)
            return {
                "code"   : 206,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error signing token with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not sign token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }

    def verify_token(self):
        json_input = request.json
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        token_json = json_input['token']
        token = jsonpickle.loads(token_json)
        try:
            self.logger.info("checking sign of token")
            self.manager.verifyToken(token)
            return {
                "code"   : 206,
                "status" : "ok",
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error checking token: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not check token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }


    def get_token_list(self, active):
        active = ast.literal_eval(active)
        status = 'inactive'
        if active is True:
            status = 'active'
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            token_list = self.manager.getTokenList(active)
            if len(token_list) != 0:
                return {
                    "code"   : 206,
                    "status" : "ok",
                    "token_list"   : jsonpickle.encode(token_list),
                    "ts"     : now.isoformat()
                }
            else:
                self.logger.info("there are no %s tokens registered in the vault" % (status))
                return {
                    "code"   : 312,
                    "status" : "there are no tokens",
                    "token_list"   : None,
                    "ts"     : now.isoformat()
                }
        except Exception as e:
            self.logger.info("error listing tokens: %s" % (e))
            return {
                "code"   : 313,
                "status" : "Could not list tokens",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }


    def get_token_expiration_date(self, label, whom=None):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            date = self.manager.getTokenExpirationDate(label, whom)
            return {
                "code"   : 206,
                "status" : "ok",
                "date"   : jsonpickle.encode(date),
                "ts"     : now.isoformat()
            }
        except TokenNotFound as e:
            return {
                "code"   : 312,
                "status" : "token not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error geting expiration date of token with label %s: %s" % (label,e))
            return {
                "code"   : 314,
                "status" : "Could not get token expiration date",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }


    def token_expiration(self, label, whom=None):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            status = self.manager.tokenExpiration(label, whom)
            return {
                "code"       : 206,
                "status"     : "ok",
                "token_status" : jsonpickle.encode(status),
                "ts"         : now.isoformat()
            }
        except TokenNotFound as e:
            return {
                "code"   : 312,
                "status" : "token not found",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except ExpiredToken as e:
            return {
                "code"   : 313,
                "status" : "token expired",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error verifying expiration of token with label %s: %s" % (label,e))
            return {
                "code"   : 314,
                "status" : "Could not verify expiration of token",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }

        
    def get_register_credential(self, label, token=None, whom=None):
        now = datetime.now(timezone.utc).astimezone(get_localzone())
        try:
            credential = self.manager.getRegisterCredential(label, token, whom)
            #credential_encode = credential.serialize()
            credential_encode = jsonpickle.encode(credential)
            return {
                "code"       : 205,
                "status"     : "ok",
                "credential" : credential_encode,
                "ts"         : now.isoformat()
            }
        except Exception as e:
            self.logger.info("error retrieving  credential with label %s: %s" % (label,e))
            return {
                "code"   : 313,
                "status" : "Could not retrieve credential",
                "error"  : "%s" % e,
                "ts"     : now.isoformat()
            }
