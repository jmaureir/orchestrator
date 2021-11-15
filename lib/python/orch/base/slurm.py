import sys
import bupacl.orch.base
import subprocess
import time
import select
import os
import re
import dill
import pickle
import json
import io

from .AVROCodec import AVROCodec
from ._async import *
from .SerializedObject import *

from taskit.frontend import FrontEnd, BackendNotAvailableError
from taskit.log import FileLogger, DEBUG, INFO, ERROR

from threading import Thread, Event

from promise import Promise
import traceback
import tempfile

from .AbstractJob import AbstractJob

import requests
import json

import traceback

def getJobInfo(job_id):

    token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2Mjg2OTY0NjIsImlhdCI6MTYyODY5NDY2Miwic3VuIjoiamNtIn0.R2jnrCcsmEA6O8WRkS2dwvx1Y6Q1mFWci4oN97qawuA"

    headers={
      "X-SLURM-USER-NAME":"jcm",
      "X-SLURM-USER-TOKEN":token
    }

    url="http://slurm.sgda.bupa.cl/slurm/v0.0.36/job/%d" % job_id

    resp = requests.get(url, headers=headers)

    resp_json = resp.json()
 
    job_data = {}
    job_data["batch_host"] = resp_json["jobs"][0]["batch_host"]
    job_data["nodes_list"] = resp_json["jobs"][0]["nodes"]

    return job_data

class Job(AbstractJob):

    SBATCH_BIN  = "/usr/bin/sbatch"
    SRUN_BIN    = "/usr/bin/srun"
    SCANCEL_BIN = "/usr/bin/scancel"

    tmp_stdout  = "./.orch_jobs"
 
    def __init__(self, params={}):
        super().__init__(params)

        self.job_id        = None
        self.step_id       = None
        self.job           = None
        self.worker_port   = None
        self.host          = None

        self._exec_hdl     = None
        self.result        = None

        self.log_handler   = None

        self.max_retry     = 180

    def __deployBackend(self,function,lock):

        #print("starting deploy of backend")

        cmd = []

        if not self.as_job:
            cmd = [ self.SRUN_BIN, ]
        else:
            cmd = [ self.SBATCH_BIN, ]

        # add this when inside a job allocation

        if not self.as_job:
            if "SLURM_JOB_ID" in os.environ:
                if self.exclusive:
                    cmd.append("--exclusive")

                cmd.append("--export=ALL")
                cmd.append("--unbuffered")
            else:
                cmd.append("--export=ALL")
                cmd.append("--unbuffered")
        else:
            cmd.append("--export=ALL")

        for key,value in self.params.items():

            if key=="cores":
                key = "cpus-per-task"
            if key=="name":
                key = "job-name"
            if key=="verbose":
                continue
            if key=="output":
                continue

            cmd.append("--%s=%s" % (key,value))

        cmd.append("%s")

        orch_py="'from bupacl.orch.base import Worker; Worker().run()'" 

        cmd_py = []

        cmd_py.append('python3')
        cmd_py.append('-u')
        cmd_py.append('-c')
        cmd_py.append(orch_py)

        std_out = ""

        if self.as_job:
            if not os.path.exists(self.tmp_stdout):
                os.makedirs(self.tmp_stdout, exist_ok=True)

            std_out = tempfile.NamedTemporaryFile(dir=self.tmp_stdout,delete=False).name
            worker_cmd = "--wait -o %s --wrap=\"%s\"" % (std_out," ".join(cmd_py))
            cmd_str = " ".join(cmd) % worker_cmd
        else:
            cmd_str = " ".join(cmd) % " ".join(cmd_py)

        #print(cmd_str)

        proc = subprocess.Popen(cmd_str, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        poll = select.poll()
        r    = proc.poll()

        if r is not None and r != 0:
            lock.set()
            raise RuntimeError("srun command failed with exit code %d" % r)

        event_mask = select.POLLIN | select.POLLERR | select.POLLHUP | select.EPOLLIN | select.EPOLLPRI

        stdout_handler = None
        if self.as_job:
            retry_count = 0
            while not os.path.exists(std_out):
                time.sleep(1)
                retry_count+=1
                if retry_count > self.max_retry:
                    raise RuntimeError("Submitted job was not executed in %d seconds" % self.max_retry)
                    return

            stdout_handler = open(std_out,"r")
        else:
            stdout_handler = proc.stdout.fileno()

        poll.register(stdout_handler, event_mask)

        regex="JOBID: ([0-9]*) STEP: ([0-9]*) PORT: ([0-9]*) HOST: (.*)"
        prog = re.compile(regex,re.MULTILINE|re.DOTALL)
        self.host="worker"

        while proc.poll() is None:
            events = poll.poll(1)
            if events is None:
                time.sleep(1)
                continue;

            for fd, event in events:
                if event & select.POLLERR:
                    poll_active = False
                    break

                if (event & select.POLLIN) or (event & select.POLLHUP):

                    data = os.read(fd, 1024)
                    if data.decode(errors='replace') == '':
                        continue
                    lines = data.strip().splitlines()
                    for line in lines:
                        if self.verbose and self.host!="worker":
                            print("** %s %d ** %s" % (self.host, self.job_id, line.decode()))

                        if self.job_log is not None and self.host!="worker":
                            if self.log_handler is None:
                                log_file = self.job_log.replace("%J", str(self.job_id))
                                if self.step_id is not None:
                                    log_file = log_file.replace("%S", str(self.step_id))
                                # create the job logfile
                                self.log_handler = open(log_file,'w')

                            self.log_handler.write("%s\n" % line.decode())

                        #TODO: capture just the first occurence and then discard further headers of prog regex
                        m = prog.match(line.decode())

                        if m is not None:
                            job_id       = int(m.group(1))
                            step_id      = int(m.group(2))
                            worker_port  = int(m.group(3))
                            self.host    = m.group(4)

                            if worker_port>0:
                                self.worker_port = worker_port
                            else:
                                lock.set()
                                raise RuntimeError("Returned Worker Port unavailable")

                            if job_id>0:
                                self.job_id = job_id
                                self.job = {}
                                
                                if self.as_job:
                                     self.job["batch_host"] = self.host
                                else:
                                    self.step_id = step_id
                                    self.job["node_list"] = self.host

                            else:
                                lock.set()
                                raise RuntimeError("Returned JobID is not a number")

                            lock.set()
                            #print("backend ready ",job_id, self.host)    
                            break
                        
                        if len(line) == 0: 
                            break

        #print("waiting for backend to finish")
        exit_code = proc.poll()

        if self.log_handler is not None:
            self.log_handler.close()

        if exit_code>0:
            if self.verbose:
                #print("backend exit_code %d" % exit_code);
                #if self.as_job:
                    #print(stdout_handler.read())
                #else:
                    #print(proc.stdout.read())
                pass
            lock.set()
        else:
            if self.as_job:
                if self.verbose:
                    #print(stdout_handler.read())
                    pass
                stdout_handler.close()
                os.remove(std_out)

    def execute(self,function,*args):
        @Async
        def _exec(function,*args):
            result = None
            lock = Event()

            # deploy the backend
            t = Thread(target=self.__deployBackend, args=(function,lock))
            t.start()
            #print("waiting for backend")
            lock.wait()
            if self.job is not None:
                #print("starting frontend")
                try:
                    if self.as_job:
                        backend_addr = self.job["batch_host"]
                    else:
                        backend_addr = self.job["node_list"]
                    
                    #print("Worker running at %s : %d " % (backend_addr,self.worker_port))
                    #log = FileLogger(sys.stdout,[ERROR])

                    frontend = FrontEnd([backend_addr],default_port=self.worker_port, codec=bupacl.orch.base.AVROCodec)

                    retry = 0
                    retry_time = 1
                    while retry<5:
                        # getting a handle for the function to work with
                        fn = None
                        try:
                            fn = function 
                        except Exception as e:
                            print("Error getting the function to call: %s" % function)
                            print(e)
                            break
                        # unwrap the function if it is wrapped in an AsyncCall
                        if isinstance(fn,ThreadAsyncMethod) or isinstance(fn,ProcessAsyncMethod):
                            fn = fn.Callable
                        # serialize the function
                        code_string = dill.dumps(fn)
                        # build the function_calller argument to pass the function and the args
                        func_handler = {"func": code_string, "name": fn.__name__, "args": args }                            
                        try:
                            h = frontend.work('function_caller',func_handler)
                            self.result = h
                            break;
                        except BackendNotAvailableError:
                            time.sleep(retry_time)
                            retry=retry+1
                        except Exception as e:
                            print("Frontend received exception from backend: ",e)
                            self.result = e
                            break;

                    #print("sending stop signal to backend %s" % backend_addr)
                    frontend.send_stop(backend_addr)
                    t.join()
                    return self.result
                except Exception as e:
                    print("exception when sending function to backend: ", e)
                    # capture the stacktrace
                    st = io.StringIO()
                    traceback.print_exc(file=st)
                    st.seek(0)
                    st_str = st.read()
                    self.result = "%s\n%s" % (e, st_str )
                    print(st_str)
                    # cancel the job
                    job_id = self.job_id
                    subprocess.check_output([self.SCANCEL_BIN,"%d" % job_id])
                    return self.result
            else:
                self.result = RuntimeError("Could not start backend.")
                return self.result

        self._exec_hdl = _exec(function,*args)

        return self._exec_hdl

