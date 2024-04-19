import shelve
import os
from multiprocessing import Lock

class PersistentDict():

    def _openDict(self):
        mode = "w"
        if not os.path.exists(self.storage_file):
            mode = "c"

        h = shelve.open(self.storage_file, flag=mode)

        return h

    def __init__(self, storage_file="./.persistentdict/storage.dbm"):
        self.storage_file = storage_file

        if not os.path.exists(os.path.dirname(storage_file)):
            os.makedirs(os.path.dirname(storage_file))

        self.lock = Lock()

    def get(self, key):

        with self.lock:
            db = self._openDict()
            value = db[key]
            db.close()

        return value

    def mput(self,key,value, debug=False):

        with self.lock:
            db = self_openDict()
            if key in db:
                tval = db[key]
                if isinstance(tval,list):
                    if debug:
                        print("key %s has already multiple values %s. adding new value" % (key, tval))
                    if value not in tval:
                        tval.append(value)
                        db[key] = tval
                else:
                    if debug:
                        print("key %s is transformed in multiple values %s" % (key, tval))

                    if tval != value:
                        nval = [ tval, value ]
                        db[key] = nval

            else:
                db[key] = value

            db.close()

        return

    def put(self,key,value):
        with self.lock:
            db = self._openDict()
            db[key] = value
            db.close()

    def exists(self, key):
        found = False
        with self.lock:
            db = self._openDict()
            found = key in db
            db.close()

        return found


    def keys(self):
        keys = []
        with self.lock:
            db = self._openDict()
            keys = db.keys()
            db.close()

        return keys

    def updateFromTable(self,table,key=None,value=None, allow_multiple_values = False, debug=False):
        for idx,row in table.iterrows():
            t_key   = row[key]
            if t_key.strip()=="":
                continue
            t_value = row[value]
            if allow_multiple_values:
                self.mput(t_key,t_value, debug=debug)
            else:
                self.put(t_key,t_value)
