
class OrchestratorAccess(object):
    def __init__(self, manager, pipeline):
        self.manager  = manager
        self.pipeline = pipeline

    def getCredential(self, label):
        token = self.manager.getToken(label=label, whom=self.pipeline.name)
        return self.manager.getRegisterCredential(label, token, whom=self.pipeline.name)

    def actionPerformed(self, evt):
        return self.manager.actionPerformed(evt)
            
    def getOwner(self):
        return self.manager.getOwner()
    
    def getPipelines(self, name, **kw_args):
        return self.manager.getPipelines(name, **kw_args)
    
    def isPipelineRegistered(self, name, **kw_args):
        return self.manager.isPipelineRegistered(name, **kw_args)
    
    def getActivePipeline(self, name):
        return self.manager.getActivePipeline(name)
    
    def activatePipeline(self, pipeline):
        return self.manager.activatePipeline(pipeline)

    def deactivatePipeline(self, pipeline):
        return self.manager.deactivatePipeline(pipeline)
    
    def deactivateAll(self, pipeline):
        return self.manager.deactivateAll(pipeline)

    def execute(self, pipeline, *args, **kw_args):
        return self.manager.execute(pipeline, *args, **kw_args)

    def createExecutor(self, pipeline, local=True, cores=1):
        return self.manager.createExecutor(pipeline, local, cores)
 
    def getExecutionList(self, name, **kw_args):
        return self.manager.getExecutionList(name, **kw_args)
    
    def getExecution(self, exec_id):
        return self.manager.getExecution(exec_id)
    
    def getLastExecution(self, name):
        return self.manager.getLastExecution(name)

    def getScheduledExecutions(self, pipeline, **kw_args):
        return self.manager.getScheduledExecutions(pipeline, **kw_args)

    def getScheduledExecutionById(self, scheduled_event_id):
        return self.manager.getScheduledExecutionById(scheduled_event_id)

    def getLastNotification(self, label):
        return self.manager.getLastNotification(label)
        
    def getNotificationList(self, label):
        return self.manager.getNotificationList(label)

    def getSubscribedPipelines(self, label):
        return self.manager.getSubscribedPipelines(label)

    def getSubscriptionsByPipeline(self, pipeline_name):
        return self.manager.getSubscriptionsByPipeline(pipeline_name)
    
    def credentialExpiration(self, label):   
        return self.manager.credentialExpiration(label)
                    
    def getCredentialExpirationDate(self, label):
        return self.manager.getCredentialExpirationDate(label)
                        
    def getTokenExpirationDate(self, label):
        return self.manager.getTokenExpirationDate(label, self.pipeline.name)
    
    def tokenExpiration(self, label):
        return self.manager.tokenExpiration(label, self.pipeline.name)
    
    def getPublicKey(self):
        return self.manager.getPublicKey()
    
    def checkProcessExpiration(self, process_name):
        return self.manager.checkProcessExpiration(process_name)
