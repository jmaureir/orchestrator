# ExecutorManager

from ..base import ActionListener, Observable
from ..base.db import DataBaseBackend
from . import Executor
from ..exceptions import InitializeError,MultipleExecutionIDFound,ExecutionIdNotFound

from email.message import EmailMessage
from email.utils import make_msgid
import pandas as pd
import smtplib
import traceback
import re
import time
import ssl

class ExecutorManager(DataBaseBackend,ActionListener, Observable):
    
    active = {}

    def sendMail(self,mail_dst,subject=None, mail_cc=[],bounce_dest=None, mail_content=None, mail_content_html=None, attachments=None):
        # compose the email

        if self.smtp_crd is None:
            print("No SMTP configured. unable to send mails")
            return False
        
        smtp_crd = self.smtp_crd
        
        sender    = smtp_crd["sender"]
        recipient = mail_dst
        rcpt = recipient + mail_cc

        if bounce_dest is None:
            bounce_dest = sender

        asparagus_cid = make_msgid()

        # bupa relay
        server = smtp_crd["smtp_host"]
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(server,context=context) as s:
            s.set_debuglevel(0)
            s.login(smtp_crd["username"],smtp_crd["password"])
            msg = EmailMessage()
            msg.set_content(mail_content)

            msg.add_alternative(mail_content_html, subtype='html')

            if attachments is not None:
                for file in attachments:

                    mime = magic.Magic(mime=True)
                    mime_type = mime.from_file(file)

                    main_type = mime_type.split("/")[0]
                    sub_type = mime_type.split("/")[1]

                    with open(file, 'rb') as content_file:
                        content = content_file.read()
                        msg.add_attachment(content, maintype=main_type, subtype=sub_type, filename=os.path.basename(file))

            print("sending mail to %s" % rcpt)

            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = recipient
            msg['Cc'] = mail_cc

            s.sendmail(bounce_dest, rcpt, msg.as_string())

            s.quit()

    def __init__(self,owner,db_conn_str="sqlite:///orchestrator.sqlite"):
        super().__init__(db_conn_str)
        self.owner = owner
        self.smtp_crd = owner.smtp_crd
        if not self.initialize(Executor):
            raise InitializeError(Executor.__tablename__)

    def create(self,pipeline, *args, **kw_args):
        executor = Executor(self, pipeline, *args, **kw_args)
        executor.addActionListener(self)
        return executor

    def getExecutorByID(self, executor_id):
        # check executor in active list
        if executor_id in self.active:
            executor = self.active[executor_id]
            return executor
        else:
            # executor not active. trying to get it from persistency backend
            try:
                executors = self.getObjects(Executor, uuid=executor_id)                
                if len(executors) == 1:
                    executors[0].__not_persistent_init__(self)
                    executors[0].addActionListener(self)
                    
                    return executors[0]
                elif len(executors) > 1:
                    raise MultipleExecutionIDFound(executor_id)
                else:
                    raise ExecutionIdNotFound(executor_id)
            except Exception as e:
                raise e
            
        return None
    
    def getExecutionList(self, pipeline_name, **kw_args):
        exec_list = []
        # get the active executions first
        for uuid, ex in self.active.items():
            if ex.name == pipeline_name:
                exec_list.append(ex)
                        
        # get the executions stored in persistency backend
        try:
            executors = self.getObjects(Executor, defer_cols=["output","error"],name=pipeline_name, **kw_args)
            exec_list = exec_list + executors
        except Exception as e:
            raise e

        return executors
    
    def getExecutionsBy(self, where, **kw_args):
        exec_list = []
        
        # get the executions stored in persistency backend
        try:
            rs = self.query("select id from executions where %s" % where)
            id_lst = []
            for r_id in rs:
                id_lst.append(r_id[0])
                  
            exec_list = self.getObjects(Executor, Executor.id.in_(id_lst), defer_cols=["output","error"])
            
        except Exception as e:
            raise e

        return exec_list
    
    def getRunningExecutions(self):
        exec_list = list(self.active.values())
        return exec_list
    
    def sendExecutionNotification(self, pipeline, exec_id, target, show="all"):
        
        show_arr = show.split(",")
        for show_item in show_arr:
            if not show_item in ["all", "output","error","report"]:
                raise(RuntimeError("sendExecutionNotification: show must be a string list (separated by ,) of the following options: all, output,error or report"))
        
        print("notifying exceution to %s" % target)
        print("pipeline:",pipeline)
        
        last_exec = self.getExecutorByID(exec_id)
        print("last execution:",last_exec)
        
        show_output    = "output" in show_arr
        show_error     = "error" in show_arr
        show_report    = "report" in show_arr
        
        if show=="all":
            show_output    = True
            show_error     = True
            show_report    = True
        
        try:
            exec_output = None
            exec_error  = None
            report      = None
            
            if show_output or show_error:
                for retry in range(0,5):
                    last_exec = self.getExecutorByID(exec_id)
                    exec_output = last_exec.getOutput()

                    if exec_output is None:
                        print("output not yet ready")
                        time.sleep(10)
                    else:
                        break

                for retry in range(0,5):
                    last_exec = self.getExecutorByID(exec_id)
                    exec_error  = last_exec.getErrors()

                    if exec_error is None:
                        print("error output not yet ready")
                        time.sleep(10)
                    else:
                        break

                # remove credentials from url like strings
                exec_output = re.sub("(.*):\/\/(.*):(.*)@(.*)","\\1://*****:******@\\4",exec_output)
                exec_error = re.sub("(.*):\/\/(.*):(.*)@(.*)","\\1://*****:******@\\4",exec_error)

            if show_report:
                if hasattr(pipeline,"report"):
                    report = pipeline.report
            
            subject="Pipeline Execution Report: %s" % pipeline.name

            mail_cnt_output  = ""
            mail_cnt_error   = ""
            mail_cnt_report  = ""
            mail_cnt_report_html = ""
            mail_cnt_output_html = ""
            mail_cnt_error_html  = ""
            
            if show_output:
                mail_cnt_output = f"""Registro de Ejecución:\n{exec_output}\n\n"""
                mail_cnt_output_html = f"""<h2>Registro de Ejecucion</h2><pre>{exec_output}</pre>"""
            if show_error: 
                mail_cnt_error  = f"""Registro de Errores:\n{exec_error}\n\n"""
                mail_cnt_error_html  = f"""<h2>Registro de Errores</h2><pre>{exec_error}</pre>"""
            if show_report:
                mail_cnt_report = f"""Reporte de Ejecución:\n{report}\n\n"""
                if isinstance(pipeline.report, pd.DataFrame):
                    mail_cnt_report_html = f"""<h2>Reporte de Ejecución</h2><pre>{report.to_html()}</pre>"""
                else:
                    mail_cnt_report_html = f"""<h2>Reporte de Ejecución</h2><pre>{report}</pre>"""
            
            mail_content=f"""{mail_cnt_report}{mail_cnt_output}{mail_cnt_error}\n------------------------------"""
            mail_content_html=f"""{mail_cnt_report_html}{mail_cnt_output_html}{mail_cnt_error_html}\n------------------------------"""
            
            self.sendMail(target,subject=subject,mail_content=str(mail_content), mail_content_html=str(mail_content_html))
            
        except Exception as e:
            print("Error notifying execution to %s" % target)
            print(e)

            trc_bk = traceback.format_exc()

            print(trc_bk)

            subject="Pipeline Execution Report: %s" % pipeline.name

            mail_content=f"""Registro de Ejecución:\n{e}\nRegistro de Errores:\n{trc_bk}\n------------------------------"""
            mail_content_html=f"""<h2>Registro de Ejecucion:</h2><pre>{e}</pre><h2>Registro de Errores:</h2><pre>{trc_bk}</pre>------------------------------"""
            self.sendMail(target,subject=subject,mail_content=str(mail_content), mail_content_html=str(mail_content_html))
            
        print("notification sent to %s" % target)

    def actionPerformed(self, evt):
        # onyl forward the event to all listeners
        Observable.actionPerformed(self,evt)
