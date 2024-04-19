# Serialized object

import json
import os
import base64

class SerializedObject:
    def __init__(self):
        self._object = None

    def serialize(self):
        self._object = self.getAsJson()
        encode_object = base64.b64encode(self._object).decode('ascii')
        return encode_object

    @classmethod
    def deserialize(cls, b64_object):
        decode_object = base64.b64decode(b64_object.encode('ascii'))
        dict_object = json.loads(decode_object)
        return cls.buildFromJson(dict_object)
        
