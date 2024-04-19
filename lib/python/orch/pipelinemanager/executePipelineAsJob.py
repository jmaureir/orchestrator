def execute_pipeline_as_local(cores, pipeline_fn, orch_access, args, kwargs):
    from ..base import Argument, asLocalJob
    @asLocalJob(cores=cores, verbose=True)
    #def run_pipeline_as_local(args):
    def run_pipeline_as_local(pipeline_fn, args, kwargs):

        import base64
        from io import StringIO
        import sys
        import traceback
        import platform
        from orch.base import Argument
        from contextlib import redirect_stdout, redirect_stderr

        import os, psutil

        #pipeline_fn, args, kwargs = args.get()
       
        p_output = StringIO()
        p_error  = StringIO()
        
        result     = None 
        success    = False
        b64_output = ""
        b64_error  = ""
       
        with redirect_stderr(p_error) as e:
            with redirect_stdout(p_output) as o:
                try:

                    print("Execution at        : ", platform.node())
                    print("executing function  :",pipeline_fn)
                    print("Orchestrator Access :",orch_access)
                    
                    # make available the orch_access instance to the pipeline
                    pipeline_fn.__globals__["orch_access"] = orch_access
                    
                    result = pipeline_fn(*args,**kwargs)
                    
                    # memory consumption of current process
                    process = psutil.Process(os.getpid())
                    orch_access.exec_info.memory = process.memory_full_info().rss
                    print("Memory Consumption :",process.memory_info().rss, "bytes") 

                    success = True
                except Exception as ex:
                    print("Exeption when running process as local job:",ex)
                    exc_info = sys.exc_info()
                    traceback.print_exception(*exc_info)

                    result = ex

                    # memory consumption of current process
                    process = psutil.Process(os.getpid())
                    print("Memory Consumption :",process.memory_info().rss, "bytes") 
                    orch_access.exec_info.memory = process.memory_info().rss
                    del exc_info

        try:

            o.flush()
            e.flush()

            output = o.getvalue()
            error  = e.getvalue()

            b64_output = base64.b64encode(output.encode('utf8'))
            b64_error  = base64.b64encode(error.encode('utf8'))
        except Exception as ex:
            print("Error encoding output:",ex)
            err_str = "%s" % ex

            o.flush()
            e.flush()

            output = o.getvalue()
            error  = e.getvalue()

            b64_error  = base64.b64encode(err_str.encode('utf8'))
            b64_output = base64.b64encode(error.encode('utf8'))
        
        return Argument((success, result, b64_output ,b64_error, orch_access.exec_info ))

    return run_pipeline_as_local(pipeline_fn, args, kwargs )


# execute the pipeline as job
def execute_pipeline_as_job(cores, pipeline_fn, orch_access, args, kwargs):
    from ..base import Argument, asJob

    #TODO: handle partition and memory in decorator with defaults 
    @asJob(cores=cores, verbose=True)
    def run_pipeline_as_job(args):
        import base64
        from io import StringIO
        import sys
        import traceback
        import platform
        from orch.base import Argument
        from contextlib import redirect_stdout, redirect_stderr
 
        pipeline_fn, args, kwargs = args.get()
       
        p_output = StringIO()
        p_error  = StringIO()
        
        result     = None 
        success    = False
        b64_output = ""
        b64_error  = ""
       
        with redirect_stderr(p_error) as e:
            with redirect_stdout(p_output) as o:
                try:

                    print("Execution at: ", platform.node())
                    print("executing function:",pipeline_fn)

                    pipeline_fn.__globals__["orch_access"] = orch_access
                    
                    result = pipeline_fn(*args,**kwargs)
                    success = True
                except Exception as ex:
                    print("Exeption when running function:",ex)
                    exc_info = sys.exc_info()
                    traceback.print_exception(*exc_info)

                    result = ex

                    del exc_info

        try:
            output = o.getvalue()
            error  = e.getvalue()

            b64_output = base64.b64encode(output.encode('utf8'))
            b64_error  = base64.b64encode(error.encode('utf8'))
        except Exception as ex:
            print("Error encoding output:",ex)
            err_str = "%s" % ex
            b64_error  = base64.b64encode(err_str.encode('utf8'))
            
        return Argument( (success, result, b64_output ,b64_error, orch_access) )

    return run_pipeline_as_job(Argument( (pipeline_fn, args, kwargs) ))
