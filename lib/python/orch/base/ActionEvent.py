
class ActionEvent(object):
    def __init__(self, *args, **kw_args):
        self.source = None
    
    def setSource(self, src):
        self.source = src
        
    def getSource(self):
        return self.source
        
    def __repr__(self):
        return "%s[source=%s]" % (type(self).__name__, type(self.source).__name__)
