
# exceptions

class InitializeError(Exception):
    def __init__(self,table_name):
        super(InitializeError, self).__init__(table_name)

class TableDoesNotExist(Exception):
    def __init__(self,table_name):
        super(TableDoesNotExist, self).__init__(table_name)

class PipelineAlreadyRegistered(Exception):
    def __init__(self,label):
        super(PipelineAlreadyRegistered, self).__init__(label)

class PipelineNotRegistered(Exception):
    def __init__(self,label):
        super(PipelineNotRegistered, self).__init__(label)

class PipelineNotFound(Exception):
    def __init__(self,label):
        super(PipelineNotFound, self).__init__(label)
        
class MultiplePipelineFound(Exception):
    def __init__(self,label):
        super(MultiplePipelineFound, self).__init__(label)
        
class MultipleActivePipelineRegistered(Exception):
    def __init__(self,label):
        super(MultipleActivePipelineRegistered, self).__init__(label)
        
class NoActivePipelineRegistered(Exception):
    def __init__(self,label):
        super(NoActivePipelineRegistered, self).__init__(label)
        
class ImplementationIsNotAFunction(Exception):
    def __init__(self,name):
        super(ImplementationIsNotAFunction, self).__init__(name)

class MultipleExecutionIDFound(Exception):
    def __init__(self,uuid):
        super(MultipleExecutionIDFound, self).__init__(uuid)
        
class ExecutionIdNotFound(Exception):
    def __init__(self,uuid):
        super(ExecutionIdNotFound, self).__init__(uuid)

class APIResponseError(Exception):
    def __init__(self,code):
        super(APIResponseError, self).__init__(code)

class PipelineExecutionError(Exception):
    def __init__(self,e):
        super(PipelineExecutionError, self).__init__(e)

class PipelineSchedulingError(Exception):
    def __init__(self,e):
        super(PipelineSchedulingError, self).__init__(e)

class PipelineNotSavedInCatalog(Exception):
    def __init__(self,name):
        super(PipelineNotSavedInCatalog, self).__init__(name)

class EventInThePast(Exception):
    def __init__(self,name, time):
        super(EventInThePast, self).__init__("%s @ %s" % (name,time))        
        
class MultipleScheduledEventFound(Exception):
    def __init__(self,uuid):
        super(MultipleScheduledEventFound, self).__init__(uuid) 
        
class NotificationNotRegistered(Exception):
    def __init__(self,label):
        super(NotificationNotRegistered, self).__init__(label)

class SubscriberNotRegistered(Exception):
    def __init__(self,label):
        super(SubscriberNotRegistered, self).__init__(label)
