
import time
from datetime import timedelta
from datetime import datetime
from dateutil import parser
from pytimeparse.timeparse import timeparse

from ..base import ActionEvent 
from ..exceptions import EventInThePast

class TimeEvent(ActionEvent):
    def __init__(self, label, base_trigger_time_str=datetime.now().strftime("%H:%M:%S"), recurrency_str=None):
        super().__init__()
        
        self.label             = label
        self.base_trigger_time = parser.parse(base_trigger_time_str)
        
        if recurrency_str is not None and recurrency_str!="":
            self.recurrency    = timeparse(recurrency_str)  # always returned in seconds
        else:
            self.recurrency    = None
    
        self.evt_time          = None
        
        self.updateEventTime()
        
    def updateEventTime(self):
        # compute the next event time from now
        # the base_trigger_time is the base reference to compute the next event time according
        # to the recurrency from current localtime
        # if base trigger time is in the past, we compute the next trigger time
        # if base trigger time is in the future, the next trigger time is the given trigger time
        
        now = datetime.now().time()
        
        if self.recurrency is None or self.recurrency == 0:
            if self.base_trigger_time.time() > now:
                self.evt_time = self.base_trigger_time
            else:
                raise EventInThePast(self.label, self.base_trigger_time)
            
        trigger_ts              = self.base_trigger_time
        trigger_time            = trigger_ts.time()
        td_now                  = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
        td_trg                  = timedelta(hours=trigger_time.hour, minutes=trigger_time.minute, seconds=trigger_time.second)

        self.evt_time           = None
        
        if td_now >= td_trg:
            td = int((td_now.total_seconds() - td_trg.total_seconds()) / self.recurrency)+1
            self.evt_time = trigger_ts + timedelta(seconds=(self.recurrency*td))
        else:
            self.evt_time = trigger_ts 
            
        self.base_trigger_time = self.evt_time
        
        print("Time Event updated:",self.evt_time)
        
        return True
    
    def isRecurrent(self):
        if self.recurrency is not None:
            return True
        return False
        
    def getRecurrency(self):
        if self.isRecurrent():
            return timedelta(seconds=self.recurrency)
        return None
        
    def trigger(self, tm):
        if self.evt_time.hour == tm.hour and self.evt_time.minute == tm.minute and self.evt_time.second == tm.second:
            return True
        return False
        
    def __repr__(self):
        return "TimeEvent[label=%s,next_evt_time=%s,recurrency=%s]" % (self.label, self.evt_time, self.getRecurrency())
