#!/usr/bin/env python3

import time
import signal
import sys
from orch.orchestrator import *
from orch.loggers import FileLogger

orch_srv = None

def signal_handler(sig, frame):
    global orch_srv
    orch_srv.stop()

signal.signal(signal.SIGINT, signal_handler)

print("Orchestrator Service")
orch_srv = OrchestratorService(address="127.0.0.1",db_conn_str="sqlite:///./orchestrator.sqlite")
orch_srv.setLogger(FileLogger("Orchestrator",config_file="etc/logging.ini",logfile="logs/orchestrator.log"))

orch_srv.start()
time.sleep(1)
orch_srv.wait()
orch_srv.stopService()
