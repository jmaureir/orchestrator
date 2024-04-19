# Encryption Key module for Credential Manager

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .exceptions import *
from .loggers import BasicLogger
from .SerializedObject import SerializedObject


class Token(BasicLogger,SerializedObject):
    """
    Token that contains part of the information
    necessary to decrypt a credential
    """
    def __init__(self, label, token, type_=1, whom=None, comment=None, creation=None, expiration=None):
        BasicLogger.__init__(self,self.__class__.__name__)
        SerializedObject.__init__(self)
        if creation is None:
            creation = datetime.now()
            creation = creation.replace(microsecond=0)
        self.__label = label
        self.__token = token
        self.__type = type_
        self.__whom = whom
        self.__comment = comment
        self.__creation = creation
        self.__expiration = expiration

        
    def getLabel(self):
        """
        get the label related to the token
        """
        return self.__label

    
    def getToken(self):
        """
        get token
        """
        return self.__token
    
    
    def getType(self):
        """
        gets the state of the token 1: plane 2: encrypted 3: b64
        """
        return self.__type

    
    def getWhom(self):
        """
        get assigned user or process related to the token
        """
        return self.__whom
    
    
    def getComment(self):
        """
        get assigned user or process related to the token
        """
        return self.__comment
    
    
    def getCreation(self):
        """
        get assigned user or process related to the token
        """
        return self.__creation
    
    
    def getExpiration(self):
        """
        get assigned user or process related to the token
        """
        return self.__expiration
    
    def getAsJson(self):
        """
        returns object in json format
        """
        return json.dumps({
            "label"      : self.__label,
            "token"      : self.__token,
            "type"       : self.__type,
            "whom"       : self.__whom,
            "comment"    : self.__comment,
            "creation"   : str(self.__creation),
            "expiration" : str(self.__expiration)
        }).encode('ascii')


    @classmethod
    def buildFromJson(cls,json):
        """
        returns representation of the object
        """
        if json["expiration"] == 'None':
            expiration= None
        else:
            expiration=datetime.strptime(json["expiration"],'%Y-%m-%d %H:%M:%S')
            
        return Token(
            label=json["label"],
            token=json["token"],
            type_=json["type"],
            whom=json["whom"],
            comment=json["comment"],
            creation=datetime.strptime(json["creation"],'%Y-%m-%d %H:%M:%S'),
            expiration=expiration
        )

    def __repr__(self):
        return "Token[label=%s,token=%s,type=%s,whom=%s,comment=%s,creation=%s,expiration=%s]" %\
        (self.__label, self.__token, self.__type, self.__whom, self.__comment, self.__creation, self.__expiration)
    
