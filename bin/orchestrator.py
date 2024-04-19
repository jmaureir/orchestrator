#!/usr/bin/env python3
#
# Orchestrator starting script
# (c) 2020-2024 Juan Carlos Maureira
#
# ======================================================
# This work has been funded and supported over time by:
# ANID Fondecyt Iniciacion 11170657 (Ex Fondecyt)
# Center for Mathematical Modeling - University of Chile
# Bupa Chile
# Fundacion Arturo Lopez Perez
# ======================================================

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

# use this dict (or credential) for enabling SMTP in orchestrator
# smtp_crd = {"
#   "sender"    : "email"
#   "smtp_host" : "ip or fqdn"
#   "username"  : "smtp username for smpt(ssl) login"
#   "password"  : "smtp password for username"
#}
smtp_crd = None

print("Orchestrator Service")
orch_srv = OrchestratorService(
    address     = "127.0.0.1",
    db_conn_str = "sqlite:///share/orch/orchestrator.sqlite", 
    smtp_crd    = smtp_crd
)
# change the logging ini file and log file paths
orch_srv.setLogger(FileLogger("Orchestrator",config_file="etc/orch/logging.ini",logfile="logs/orch/orchestrator.log"))

orch_srv.start()
time.sleep(1)
orch_srv.wait()
orch_srv.stopService()
