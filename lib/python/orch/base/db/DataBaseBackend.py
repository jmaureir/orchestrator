
# DataBaseBackend 

import sqlalchemy as sal
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

class DataBaseBackend(object):
        
    def __init__(self, conn_str):
        try:
            self.engine = sal.create_engine(conn_str)
        except Exception as e:
            raise e
            
        session_factory = sessionmaker(self.engine,expire_on_commit=False)
        self.session = scoped_session(session_factory)
            
    def getEngine(self):
        return self.engine
    
    def existTable(self,table_name):
        engine = self.getEngine()
        ins = sal.inspect(engine)
        if not ins.has_table(table_name):
            return False
        return True
    
    def getTable(self,table_name):
        if self.existTable(table_name):
            engine = self.getEngine()            
            metadata = sal.MetaData(engine)
            table = sal.Table(table_name, metadata, autoload=True, autoload_with=engine)
        
            return table
        raise TableDoesNotExist(table_name)
        
    def initialize(self, p_object):
        if not self.existTable(p_object.__tablename__):
            engine = self.getEngine()
            try:
                # Implement the creation
                p_object.metadata.create_all(engine)
            except Exception as e:
                print(e)
                return False
            return True
        
        return True
    
    def getObjects(self, p_obj, *args, **kwargs):
        sess = self.session()
        rs = None
        try:
            if len(kwargs)>0:
                rs = sess.query(p_obj, *args).filter_by(**kwargs).all()
            else:
                rs = sess.query(p_obj, *args).all()
        except Exception as e:
            raise e
        finally:
            self.session.remove()
        
        return rs
        
    def refreshObject(self, p_obj):
        sess = self.session()
        try:
            sess.expire(p_obj)
            sess.refresh(p_obj)            
        except Exception as e:
            raise e        
        finally:
            self.session.remove()
            
        return True
    
    def commit(self):
        sess = self.session()
        try:
            sess.commit()
        except Exception as e:
            raise e
        finally:
            self.session.remove()
        return True
    
    def updateObjects(self, p_obj, *args, **kw_args):
        sess = self.session()
        try:
            rs = sess.query(p_obj).filter(*args).update(kw_args)
            sess.commit()
        except Exception as e:
            raise e
        finally:
            self.session.remove()
            
        return rs
    
    def saveObject(self, p_obj):
        sess = self.session()
        try:
            sess.add(p_obj)
            sess.commit()
        except Exception as e:
            raise e
        finally:    
            self.session.remove()
    
        return True

    def destroyObject(self, p_obj):
        sess = self.session()
        try:
            sess.delete(p_obj)
            sess.commit()
        except Exception as e:
            raise e
        finally:    
            self.session.remove()

        return True
