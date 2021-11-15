from ..base import ActionEvent

class ExecutionStarted(ActionEvent):
    def __init__(self,*args,**kw_args):
        super().__init__()
        
class ExecutionFinished(ActionEvent):
    def __init__(self,*args,**kw_args):
        super().__init__()
