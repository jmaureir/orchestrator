from threading import Thread
import time
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse

from ..base import Observable
from . import TimeEvent

class TimeManager(Thread, Observable):
    def __init__(self, resolution="1s"):
        Thread.__init__(self)
        Observable.__init__(self)
        try:        
            self.resolution = timeparse(resolution)
        except Exception as e:
            raise e
        self.running = False
        
        self.event_list = {}
        
        self.start()
        self.localtime = datetime.now()

    def addTimeEvent(self, label, trigger_time_str, recurrency_str=None):        
        self.event_list[label] = TimeEvent(label, trigger_time_str, recurrency_str)
        
    def removeTimeEvent(self, evt_label):
        if evt_label in self.event_list:
            del self.event_list[evt_label] 
            return True
        
        return False
        
    def stop(self):
        self.running = False
        
    def run(self):
        
        print("TimeManager starting time:",datetime.now())
        self.running = True
        while self.running:
            time.sleep(self.resolution)
            self.localtime = datetime.now()
            for evt_label, evt in list(self.event_list.items()):
                if evt.trigger(self.localtime):
                    if evt.isRecurrent():
                        # update next event time
                        self.event_list[evt_label].updateEventTime()
                        
                    else:
                        # one time event. removing it from event list
                        del self.event_list[evt_label]
                        
                    # trigger the event to the listeners
                    self.actionPerformed(evt)
        print("TimeManager ending time:",datetime.now())
        
