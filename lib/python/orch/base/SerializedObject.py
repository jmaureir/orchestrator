class SerializedObject(object):
    _data =  {}
    def __init__(self,data={}):
        self._data = data

    def __del__(self):
        del self._data

    def get(self,key):
        return self._data[key]
