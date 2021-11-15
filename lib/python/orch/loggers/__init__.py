import logging
from logging import warning, info
from logging.config import fileConfig
import sys

class NullLogger:
    def __init__(self):
        pass
    def info(*args,**kwargs):
        pass
    def warning(*args,**kwargs):
        pass
    def error(*args,**kwargs):
        pass
    def critical(*args,**kwargs):
        pass
    def debug(*args,**kwargs):
        pass
    
class BasicLogger:
    def __init__(self, name):
        self.name = name
        logging.basicConfig(format='%(asctime)s | %(levelname)s : %(message)s',level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(self.name)
        
    def info(self,*args,**kwargs):
        self.logger.info(*args,**kwargs)
    def critical(self,*args,**kwargs):
        self.logger.critical(*args,**kwargs)
    def warning(self,*args,**kwargs):
        self.logger.warning(*args,**kwargs)
    def error(self,*args,**kwargs):
        self.logger.error(*args,**kwargs)
    def debug(self,*args,**kwargs):
        self.logger.debug(*args,**kwargs)

class FileLogger:
    def __init__(self, name, config_file="etc/logging.ini", logfile="logs/orchestrator.log"):
        self.name = name
        fileConfig(config_file)
        self.logger = logging.getLogger(self.name)

    def info(self,*args,**kwargs):
        self.logger.info(*args,**kwargs)
    def critical(self,*args,**kwargs):
        self.logger.critical(*args,**kwargs)
    def warning(self,*args,**kwargs):
        self.logger.warning(*args,**kwargs)
    def error(self,*args,**kwargs):
        self.logger.error(*args,**kwargs)
    def debug(self,*args,**kwargs):
        self.logger.debug(*args,**kwargs)
