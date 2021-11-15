
from datetime import datetime
from dateutil import parser

from ..base import ActionListener, Observable
from ..base.db import DataBaseBackend
from ..exceptions import MultipleScheduledEventFound
from .TimeManager import TimeManager
from .TimeEvent import TimeEvent
from .ScheduledEvent import ScheduledEvent
from .Events import *

class SchedulerManager(DataBaseBackend, ActionListener, Observable):
    def __init__(self,owner, time_resolution="1s", db_conn_str="sqlite:///orchestrator.sqlite"):
        Observable.__init__(self)
        self.owner = owner
        self.tm    = TimeManager(time_resolution)

        self.tm.addActionListener(self)
        super().__init__(db_conn_str)
        
        if not self.initialize(ScheduledEvent):
            raise InitializeError(ScheduledEvent.__tablename__)
            
        # activate current scheduled pipelines
        self.activateScheduledPipelines()
            
    def activateScheduledPipelines(self):
        print("activating current scheduled events")
        active_scheduled_events = self.getObjects(ScheduledEvent, active=True)
        for sch_evt in active_scheduled_events:
            trigger_time_str = sch_evt.trigger_time.strftime("%H:%M:%S")
            print("scheduling %s" % sch_evt)
            self.tm.addTimeEvent(sch_evt.uuid,trigger_time_str,sch_evt.recurrency)

    def scheduleAt(self, pipeline, label = None, trigger_time_str=datetime.now().strftime("%H:%M:%S"), recurrency=None, tags=[]):
        
        if label is None:
            label = pipeline.name
        
        sch_evt      = ScheduledEvent(self,label)
        trigger_time = parser.parse(trigger_time_str)
        
        sch_evt.owner_id      = self.owner.getOwner()
        sch_evt.tags          = ",".join(tags)
        sch_evt.trigger_time  = trigger_time
        sch_evt.recurrency    = recurrency
        sch_evt.pipeline      = pipeline.name
        
        if hasattr(pipeline,"args"):
            print("execution args:",pipeline.args)
            sch_evt.setArguments(pipeline.args)
        else:
            sch_evt.setArguments(tuple())
            
        if hasattr(pipeline,"kw_args"):
            print("execution kw_args:",pipeline.kw_args)
            sch_evt.setKeywordArguments(pipeline.kw_args)
        else:
            sch_evt.setKeywordArguments({})
            
        sch_evt.active        = True
        try:
            if self.saveObject(sch_evt):
                self.tm.addTimeEvent(sch_evt.uuid,trigger_time_str,recurrency)
                return True
        except Exception as e:
            print("could not schedule the pipeline")
            raise e

    def cancelEvent(self, uuid):
        sch_evt_list = self.getObjects(ScheduledEvent, uuid=uuid)
            
        if len(sch_evt_list)==1:
            sch_evt = sch_evt_list[0]
            sch_evt.active = False
            
            if self.saveObject(sch_evt):
                self.tm.removeTimeEvent(uuid)
                return True
            
            return False
        elif len(sch_evt_list)>1:
            print("FATAL: multiple scheduled pipelines under the same uuid",uuid)
        else:
            print("no ScheduledEvent found for uuid",uuid)
        return False
            
    def actionPerformed(self, evt):
        print("SchedulerManager: actionEvent arrived", evt)
        sch_evt_list = self.getObjects(ScheduledEvent, uuid=evt.label)
        if len(sch_evt_list)==1:
            sch_evt = sch_evt_list[0]
            if sch_evt.active:
            
                args = sch_evt.getArguments()
                kw_args = sch_evt.getKeywordArguments()

                if not evt.isRecurrent():
                    print("scheduled event not recurrent.")
                    sch_evt.active = False
                    self.saveObject(sch_evt)

                Observable.actionPerformed(self,ExecutePipeline(sch_evt.uuid, sch_evt.pipeline, args, kw_args))
            else:
                print("SchedulerManager: scheduled event not active. ignoring time event")
                
        elif len(sch_evt_list)>1:
            print("multiple scheduled pipelines under the same uuid.")
        else:
            print("no ScheduledEvent found for ",evt)
            
    def getScheduledEvents(self, pipeline, **kw_args):
        sch_evt_list = self.getObjects(ScheduledEvent, pipeline=pipeline.name, **kw_args)
        return sch_evt_list
    
    def getScheduledEventById(self, scheduled_event_id):
        sch_evt_list = self.getObjects(ScheduledEvent, uuid=scheduled_event_id)
        if len(sch_evt_list)==1:
            return sch_evt_list[0]
        elif len(sch_evt_list)==0:
            return None
        else:
            raise MultipleScheduledEventFound(scheduled_event_id)
    
    def stop(self):
        self.tm.stop()
