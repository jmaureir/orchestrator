import select 
import os 
import re 
import jsonpickle as jp
from .SerializedObject import SerializedObject

# CLEAN THIS CLASS FROM SCHEMA RELATED TO ASTRONOMY

class AVROCodec(object):
 
    """
    AVRO codec using jsonpickle for encoding. 
    """

    hdu_schema = '{"namespace": "example.avro",\
        "type": "record",\
        "name": "FITS-HDU",\
        "fields": [\
            {"name": "image"       ,"type": "bytes"},\
            {"name": "header"      ,"type": "bytes"}\
        ]\
    }'
    
    @staticmethod
    def encode(obj):
        #print("encoding ",obj, type(obj))

        if isinstance(obj,bool):
            #print("encoding bool")
            s = SerializedObject({"type":type(obj), "value": str(obj) })
            return jp.dumps(s)

        if isinstance(obj,str):
            #print("encoding string")
            s = SerializedObject({"type":type(obj), "value": obj })
            return jp.dumps(s)

        if isinstance(obj,int):
            #print("encoding int")
            s = SerializedObject({"type":type(obj), "value": str(obj) })
            return jp.dumps(s)

        if isinstance(obj, (list, tuple) ):
            #print("encoding list")
            l = [AVROCodec.encode(o) for o in obj]
            return jp.dumps(l)

        #print("encoding obj ",obj, type(obj))
        #s = time.time()
        l = jp.dumps(obj)
        #e = time.time()
        #print("done encoding %s" % (e-s))
        return l

    @staticmethod
    def decode(enc):
        obj = None
        try:
            obj = jp.loads(enc)
        except Exception as e:
            print(e)

        if isinstance(obj, (list, tuple)):
            #print("decoding list")
            return [ AVROCodec.decode(o) for o in obj ]

        if isinstance(obj,SerializedObject):
            #print("decoding serializedObject")
            o = obj.get("type")()

            if isinstance(o,bool):
                return obj.get("value")

            if isinstance(o,int):
                return int(obj.get("value"))

            if isinstance(o,str):
                return str(obj.get("value"))

        return obj

