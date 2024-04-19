import time
from datetime import timedelta
from datetime import datetime
from dateutil import parser
from pytimeparse.timeparse import timeparse
from dateutil.relativedelta import relativedelta

from ctparse import ctparse
from ctparse.types import Time as ctTime
from ctparse.types import Duration as ctDuration
from ctparse.types import DurationUnit

from ..base import ActionEvent 
from ..exceptions import EventInThePast

class TimeEvent(ActionEvent):

    def getTime2Trigger(self,when, recurrency=None, reference=datetime.now()):
        evt_time = when
        if recurrency is not None:
            evt_time = "%s %s" % (when, recurrency)
        
        ctr = ctparse(evt_time,reference).resolution
        next_evt_date = None
        if isinstance(ctr,ctDuration):
            #print("duration",ctr, ctr.unit)
            td = None
            t = 0
            if ctr.unit == DurationUnit.MONTHS:
                td = relativedelta(months=ctr.value)
                t = 1 
            elif ctr.unit == DurationUnit.DAYS:
                td = relativedelta(days=ctr.value)
                t = 1
            elif ctr.unit == DurationUnit.HOURS:
                td = relativedelta(hours=ctr.value)
            elif ctr.unit == DurationUnit.MINUTES:
                td = relativedelta(minutes=ctr.value)
            else:
                print("DurationUnit not handled",ctr.unit)

            ref = parser.parse(when)
            if t == 0:
                next_evt_date = reference + td
            else:
                if ref > reference:
                    next_evt_date = ref
                else:
                    next_evt_date = ref + td

        elif isinstance(ctr,ctTime):
            #print("time",ctr)
            if ctr.hasTime:
                next_evt_date = datetime(year=ctr.year, month=ctr.month,day=ctr.day, hour=ctr.hour, minute=ctr.minute)
            else:
                when_tm = time.strptime(when,"%H:%M")
                next_evt_date = datetime(year=ctr.year, month=ctr.month,day=ctr.day, hour=when_tm.tm_hour, minute=when_tm.tm_min)

        return next_evt_date

    def __init__(self, label, when=datetime.now(), recurrency_str=None, resolution=1):
        super().__init__()
        
        self.label             = label
        self.when              = when
        
        if recurrency_str is not None and recurrency_str!="":
            self.recurrency    = recurrency_str
        else:
            self.recurrency    = None
    
        self.evt_time          = None
        self.resolution        = resolution

        try:
            self.evt_time          = self.getTime2Trigger(self.when, self.recurrency)
            print("Event scheduled at",self.evt_time)
        except Exception as e:
            print("error computing next event time")

    def updateEventTime(self):
      
        print("updating event time")
        tm = datetime.now()
        try:
            next_evt_ts = self.getTime2Trigger(self.when, self.recurrency ,tm)

            print("evt_time      ",self.evt_time)
            print("next_evt_time ",next_evt_ts)

            if self.evt_time is None:
                self.evt_time = parser.parse(self.when)

            if next_evt_ts > tm:
                self.evt_time = next_evt_ts
                print("Time Event updated:",self.evt_time)
            else:
                print("next event time in the past",next_evt_ts, tm)

            return next_evt_ts
        except Exception as e:
            print("error computing next event time")

        return None

    def isRecurrent(self):
        if self.recurrency is not None:
            return True
        return False
        
    def getRecurrency(self):
        if self.isRecurrent():
            try:
                evt_time = "%s %s" % (self.when, self.recurrency)
                ctr = ctparse(evt_time,datetime.now()).resolution
                return ctr
            except Exception as e:
                return self.recurrency
        return None
        
    def trigger(self, tm):
        try:
            if self.evt_time < tm:
                return True
        except Exception as e:
            print("error determining triggering condition for",self)

        return False
        
    def __repr__(self):
        return "TimeEvent[label=%s,next_evt_time=%s,recurrency=%s]" % (self.label, self.evt_time, self.getRecurrency())
