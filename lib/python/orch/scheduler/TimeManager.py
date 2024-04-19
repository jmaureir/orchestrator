from threading import Thread
import time
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
import re

from ..base import Observable
from . import TimeEvent
from ..exceptions import EventWithNoRecurrence

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
        # translate short recurrency format to long one
        re_dur  = "([0-9]*)(m|h|d|M)"
        t = None
        
        if recurrency_str is not None:
            if re.match(re_dur, recurrency_str):
                match = re.search(re_dur, recurrency_str)
                if match.group(2)=="m":
                    t = "%d minutes" % int(match.group(1))
                if match.group(2)=="h":
                    t = "%d hours" % int(match.group(1))
                if match.group(2)=="d":
                    t = "%d days" % int(match.group(1))
                if match.group(2)=="M":
                    t = "%d months" % int(match.group(1))
                recurrency_str = t

        self.event_list[label] = TimeEvent(label, trigger_time_str, recurrency_str, self.resolution)
        
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
                try:
                    if evt.trigger(self.localtime):
                        print("triggerred",evt)
                        if evt.isRecurrent():
                            # update next event time
                            self.event_list[evt_label].updateEventTime()
                            
                        else:
                            print("one time event list. removing it")
                            # one time event. removing it from event list
                            del self.event_list[evt_label]
                            
                        # trigger the event to the listeners
                        self.actionPerformed(evt)

                except Exception as e:
                    print("TimeManager Exception:",e)
                    print("Event:",evt)

        print("TimeManager ending time:",datetime.now())
