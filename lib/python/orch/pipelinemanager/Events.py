from ..base import ActionEvent

class ExecutionStarted(ActionEvent):
    def __init__(self,pipeline, *args,**kw_args):
        super().__init__()
        self.pipeline_name = pipeline.name
        self.pipeline      = pipeline
        
class ExecutionFinished(ActionEvent):
    def __init__(self,pipeline,*args,**kw_args):
        super().__init__()
        self.pipeline_name = pipeline.name
        self.pipeline      = pipeline
