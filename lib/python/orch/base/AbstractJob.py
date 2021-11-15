class AbstractJob(object):

    def __init__(self,params={}):
        self.verbose   = False
        self.params    = params
        self.result    = None
        self.exclusive = True

        self.as_job    = None
        self.job_log   = None

        self._exec_hdl = None

    def getResult(self):
        if isinstance(self.result,RuntimeError):
            raise self.result

        return self.result

    def setExclusive(self, b):
        self.exclusive = b

    def setVerbose(self,v):
        self.verbose = v

    def setJobLog(self, log_file):
        self.job_log = log_file
        
    def asJob(self, b):
        self.as_job = b


    def run(self, function, *args):
        return self.execute(function,*args)

    def execute(self,function,*args):
        raise RuntimeError("AbstractJob: execute Not Implemented")

    def join(self):
        return self._exec_hdl.wait()

    def wait(self):
        return self.join()

    def __del__(self):
        pass

