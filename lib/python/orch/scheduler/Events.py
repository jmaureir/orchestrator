from ..base import ActionEvent

class ExecutePipeline(ActionEvent):
    def __init__(self,sch_evt_uuid, pipeline, args, kw_args):
        super().__init__()
        self.sch_evt_uuid = sch_evt_uuid
        self.pipeline     = pipeline
        self.args         = args
        self.kw_args      = kw_args
    def __repr__(self):
        return "<ExecutePipeline[sch_evt_uuid=%s, pipeline=%s, args=%s, kw_args=%s]>" % (
            self.sch_evt_uuid,
            self.pipeline,
            self.args,
            self.kw_args
        )
