import pickle
import os
import tempfile
import jsonpickle as jp
import dill

class Argument:


    def __init__(self, value, persistent = False, pkl_path = "./tmp"):
        self.value = value
        self.persistent = persistent
        self.got   = 0
        self.pkl_path = pkl_path

        if not os.path.exists(self.pkl_path):
            os.makedirs(self.pkl_path)

        self.pkl_file = tempfile.NamedTemporaryFile(dir=self.pkl_path,delete=False).name
        #pickle.dump(jp.dumps(self.value), )
        with open(self.pkl_file, 'wb') as fd:
            dill.dump(self.value,fd)
        self.value = None

    def __del__(self):
        if not self.persistent and os.path.exists(self.pkl_file) and self.got:
            os.remove(self.pkl_file)

    def type(self):
        if self.value is not None:
            return type(self.value)
        return None

    def destroy(self):
         if os.path.exists(self.pkl_file):
            os.remove(self.pkl_file)

    def saveAs(self, outfile):
        from shutil import copyfile
        copyfile(self.pkl_file, outfile)
        
    def get(self):
        with open(self.pkl_file,'rb') as fd:
            self.value = dill.load(fd) 
    
        self.got = 1
        return self.value
