import sys
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
import pandas as pd

class SlurmController(object):
    BATCH_BIN  = "/usr/bin/sbatch"
    SRUN_BIN    = "/usr/bin/srun"
    SCANCEL_BIN = "/usr/bin/scancel"
    SQUEUE_BIN  = "/usr/bin/squeue"

    def __init__(self, job):
        self.job = job

    def squeue(self, steps=False):

        squeue_args = ['-o','%all']
        if steps:
            squeue_args.append('-s')

        result = subprocess.run([self.SQUEUE_BIN] + squeue_args, stdout=subprocess.PIPE, shell=False, universal_newlines = True)
        output = result.stdout

        lines = output.split('\n')
        header_line = lines.pop(0)
        header_cols = header_line.split('|')
        entries = []
        error_lines = [] # do something with this later
        for line in lines:
            parts = line.split('|')
            d = {}
            if len(parts) != len(header_cols):
                error_lines.append((len(parts), line, parts))
            else:
                for i, key in enumerate(header_cols):
                    if key not in d:
                        d[key] = parts[i]
            if d:
                entries.append(d)
        df = pd.DataFrame(entries, columns=header_cols)
        df = df.loc[:,~df.columns.duplicated()]
        return df

    def cancel(self, job_id, steps=False):
        df = self.squeue(steps=steps)
        if df.loc[df.JOBID==job_id].shape[0]>0:
            result = subprocess.run([self.SCANCEL_BIN,job_id], stdout=subprocess.PIPE, shell=False, universal_newlines = True)
            if result.returncode==0:
                return True
            return False
        else:
            # job_id not in squeue
            return False

    def getJobInfo(self, job_id, steps=False):
        df = self.squeue(steps=steps)

        df_job = df.loc[df.JOBID==job_id]
        if df_job.shape[0]>0:
            return json.loads(df_job.iloc[0].to_json())

        return None

    def getJobByName(self,job_name, last=False, steps=False):
        df = self.squeue(steps=steps)
        df_job = df.loc[df.NAME==job_name]
        if df_job.shape[0]>0:
            return json.loads(df_job.to_json(orient="records"))

        return None

class Job(AbstractJob):

    SBATCH_BIN  = "/usr/bin/sbatch"
    SRUN_BIN    = "/usr/bin/srun"
    SCANCEL_BIN = "/usr/bin/scancel"
    SQUEUE_BIN  = "/usr/bin/squeue"

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
        self.exception     = None

    def __deployBackend(self,function,lock):

        print("starting deploy of backend")

        cmd = []

        steps = False
        if not self.as_job:
            cmd = [ self.SRUN_BIN, ]
            steps = True
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

        cmd_py.append('python3.9')
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

        #print("cmd:",cmd_str)
        #print("as_job:",self.as_job)

        proc = subprocess.Popen(cmd_str, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        poll = select.poll()
        r    = proc.poll()

        if r is not None and r != 0:
            self.exception=RuntimeError("srun command failed with exit code %d" % r)
            lock.set()
            return

        event_mask = select.POLLIN | select.POLLERR | select.POLLHUP | select.EPOLLIN | select.EPOLLPRI

        # check for node allocation status
        slurm_ctrl = SlurmController(self)
        #print(self.params)

        if self.as_job:
           print("running as job") 

        for retry in range(0,10):
            #print("waiting job info %s" % self.params["job-name"])
            job_info_lst = slurm_ctrl.getJobByName(self.params["job-name"],steps=steps)
            if job_info_lst is None:
                time.sleep(10)
            else:
                break

        if job_info_lst is None:
            self.exception=RuntimeError("Could not get jobinfo from slurm")
            lock.set()
            return 

        job_info = job_info_lst[-1]  # get the last job in the list
        job_id = None
        if self.as_job:
            job_id = job_info["JOBID"]
        else:
            job_id = job_info["STEPID"]

        wait_time = 10 # segs
        is_running = False
        if self.as_job:
            for retry in range(0,60):
                print("waiting for job")
                job_info = slurm_ctrl.getJobByName(self.params["job-name"],steps=steps)[-1]  # get the last job in the list
                #print("job_info:",job_info)
                if job_info["STATE"]=="RUNNING":
                    is_running = True
                    break
                time.sleep(wait_time)

            if not is_running:
                print("Worker job not running after 600 segs")
                slurm_ctrl.cancel(job_id)
                self.exception = RuntimeError("Worker job not running after 300 segs")
                lock.set()
                return
        else:
            #job step
            pass

        stdout_handler = None
        if self.as_job:
            retry_count = 0
            while not os.path.exists(std_out):
                time.sleep(1)
                retry_count+=1
                if retry_count > self.max_retry:
                    self.exception=RuntimeError("Submitted job was not executed in %d seconds" % self.max_retry)
                    lock.set()
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
                os.fsync(fd)
                if (event & select.POLLIN) or (event & select.POLLHUP):
                    data = os.read(fd, 10240)
                    if data.decode(errors='replace') == '':
                        continue
                    lines = data.strip().splitlines()
                    for line in lines:
                        if self.verbose and self.host!="worker":
                            print("** %s %d ** %s" % (self.host, self.job_id, line.decode()),flush=True)

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
                                self.exception=RuntimeError("Returned Worker Port unavailable")
                                lock.set()
                                return

                            if job_id>0:
                                self.job_id = job_id
                                self.job = {}

                                if self.as_job:
                                     self.job["batch_host"] = self.host
                                else:
                                    self.step_id = step_id
                                    self.job["node_list"] = self.host

                            else:
                                self.exception=RuntimeError("Returned JobID is not a number")
                                lock.set()
                                return

                            lock.set()
                            print("backend ready ",job_id, self.host)
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
            print("waiting for backend")
            lock.wait()
            if self.job is not None:
                print("starting frontend")
                try:
                    if self.as_job:
                        backend_addr = self.job["batch_host"]
                    else:
                        backend_addr = self.job["node_list"]

                    print("Worker running at %s : %d " % (backend_addr,self.worker_port))
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

                    print("sending stop signal to backend %s" % backend_addr)
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
                    print("error read from backend:",st_str)
                    # cancel the job
                    job_id = self.job_id
                    subprocess.check_output([self.SCANCEL_BIN,"%d" % job_id])
                    return self.result
            else:
                self.result = self.exception
                return self.result

        self._exec_hdl = _exec(function,*args)

        return self._exec_hdl
