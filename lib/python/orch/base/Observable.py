
from . import ActionListener

class Observable(object):
    def __init__(self,*args, **kw_args):
        self.listeners = []
        
    def addActionListener(self,al):
        if issubclass(type(al), ActionListener):
            if not hasattr(self,"listeners"):
                self.listeners = []
            self.listeners.append(al)
            return self
        raise RuntimeError("%s must inhnerit from ActionListener" % type(al).__name__)

    def actionPerformed(self, evt):
        evt.setSource(self)
        if not hasattr(self,"listeners"):
            self.listeners = []
            
        for al in self.listeners:
            al.actionPerformed(evt)
        
