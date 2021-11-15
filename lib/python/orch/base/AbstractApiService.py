
from flask import Flask, render_template, request, jsonify
from threading import Thread, Condition
from werkzeug.serving import make_server

from ..loggers import NullLogger

class AbstractApiService(Thread):
    def __init__(self,api_name, bind_addr="127.0.0.1", bind_port=8010, templates=None):
        Thread.__init__(self)
        self.api_name  = api_name
        self.bind_addr = bind_addr
        self.bind_port = bind_port

        self.api       = Flask(api_name, template_folder=templates)
        self.srv       = None
        self.logger    = NullLogger()
        self.running   = False

        self.cv        = None
        
    def __del__(self):
        self.stopService()
    
    def isRunning(self):
        return self.running
        
    def setLogger(self, logger):        
        self.logger = logger

    def addRule(self, url, name, callback, **kwargs):
        self.api.add_url_rule(url, name, callback,**kwargs)

    def wait(self):
        self.cv = Condition()
        with self.cv:
            self.cv.wait()
            return True
    
    def release(self):
        if self.cv is not None:
            with self.cv:
                self.cv.notifyAll()
    
    def stopService(self):
        if self.srv is not None:
            self.srv.shutdown()

        # wait for thread to finish
        self.join()
        
        self.logger.info("%s : stopped" % self.api_name)
        self.running = False
        
    def run(self):
        self.logger.info("%s : starting" % self.api_name)
        self.srv  = make_server(self.bind_addr, self.bind_port, self.api)
        self.ctx  = self.api.app_context()
        self.running = True
        self.srv.serve_forever()
        self.logger.info("%s : finishing" % self.api_name)
