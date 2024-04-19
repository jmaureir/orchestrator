# Credential module for Credential Manager

import json
import os
import base64
import ast
import uuid as _uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .exceptions import *
from .loggers import BasicLogger
from .SerializedObject import SerializedObject
from .ShamirImplementation import *

class Credential(BasicLogger,SerializedObject):
    credential_path = "./.credentials"
    def __init__(self, label, content, uuid=None, nonce=None, tag=None, token=None, type_=1, creation=None, expiration=None):
        if creation is None:
            creation = datetime.now()
            creation = creation.replace(microsecond=0)
        if expiration is None:
            expiration = creation + relativedelta(days=365)
        self.label = label
        if uuid is None:
            self.uuid  = str(_uuid.uuid4())
        else:
            self.uuid = uuid
        self.content = content
        self.nonce = nonce
        self.tag = tag
        self.token = token
        self.type_ = type_
        self.creation = creation
        self.expiration = expiration
        BasicLogger.__init__(self,self.__class__.__name__)
        SerializedObject.__init__(self)
    
    @classmethod
    def load(cls, label, key):
        """
        Loads credential from local source
        param: label, the label of credential
        """
        credential_file = "%s/%s.credential" % (cls.credential_path, label)
        os.makedirs(cls.credential_path, exist_ok=True)
        if os.path.exists(credential_file):
            try:
                with open(credential_file, 'r') as file:
                    dict_credential = json.load(file)
                    credential_type = dict_credential["type"]
                    credential_creation = datetime.strptime(dict_credential["creation"],'%Y-%m-%d %H:%M:%S')
                    credential_expiration = datetime.strptime(dict_credential["expiration"],'%Y-%m-%d %H:%M:%S')
                    if type(dict_credential["content"]) == dict:
                        credential_label = dict_credential["label"]
                        credential_content = dict_credential["content"]
                        credential_uuid = dict_credential["uuid"]
                        credential_nonce = dict_credential["nonce"]
                        credential_tag = dict_credential["tag"]
                        credential_token = dict_credential["token"]
                    else:
                        credential_label = dict_credential["label"]
                        if key is not None:                            
                            credential_content_decode_prev = base64.b64decode(dict_credential["content"].encode("ascii"))
                            credential_content_decrypt = key.decrypt(credential_content_decode_prev)
                            credential_content = base64.b64decode(credential_content_decrypt)  
                        else:
                            credential_content_encode = dict_credential["content"].encode("ascii")
                            credential_content = base64.b64decode(credential_content_encode)
                        credential_nonce_encode = dict_credential["nonce"].encode("ascii")
                        credential_nonce = base64.b64decode(credential_nonce_encode)
                        credential_tag_encode = dict_credential["tag"].encode("ascii")
                        credential_uuid = dict_credential["uuid"].encode("ascii")
                        credential_tag = base64.b64decode(credential_tag_encode)
                        credential_token_encode_index = dict_credential["token"][0].encode("ascii")
                        credential_token_encode_token = dict_credential["token"][1].encode("ascii")
                        credential_token = (int(base64.b64decode(credential_token_encode_index)),base64.b64decode(credential_token_encode_token)) 
                    
                    
                    return Credential(credential_label, credential_content, uuid=credential_uuid, nonce=credential_nonce, tag=credential_tag, token=credential_token,\
                                      type_=credential_type, creation=credential_creation, expiration=credential_expiration)
            except Exception as e:
                raise (e)
        else:
            raise (LocalCredentialNotFound(label))
    
    def save(self):
        """
        Saves the encrypted data into the credentials file
        """
        os.makedirs(self.credential_path, exist_ok=True)
        credential_file = "%s/%s.credential" % (self.credential_path, self.label)
        if not os.path.exists(credential_file):
            with open(credential_file, 'w+', encoding='utf-8') as cf:
                dict_credential = {}
                if type(self.content) == dict:
                    self_content = self.content
                    self_uuid = self.uuid
                    self_nonce = self.nonce
                    self_tag = self.tag
                    self_token = self.token
                    self_type = self.type_
                    self_creation = self.creation
                    self_expiration = self.expiration
                else:
                    self_content = self.content
                    self_uuid = base64.b64encode(self.uuid).decode("ascii")
                    self_nonce = base64.b64encode(self.nonce).decode("ascii")
                    self_tag = base64.b64encode(self.tag).decode("ascii")
                    self_token = (base64.b64encode(bytes(str(self.token[0]), 'ascii')).decode("ascii"),base64.b64encode(self.token[1]).decode("ascii"))
                    self_type = self.type_
                    self_creation = self.creation
                    self_expiration = self.expiration

                        
                dict_credential["label"] = self.label
                dict_credential["content"] = self_content
                dict_credential["uuid"] = self_uuid
                dict_credential["nonce"] = self_nonce
                dict_credential["tag"] = self_tag
                dict_credential["token"] = self_token
                dict_credential["type"] = self_type
                dict_credential["creation"] = str(self_creation)
                dict_credential["expiration"] = str(self_expiration)
                json.dump(dict_credential, cf)
        else:
            raise (AlreadyExists("Credential", self.label))

    def __getitem__(self, key):
        if key in self.content:
            return self.content[key]
        return None

    def getLabel(self):
        """
        Get the label of the credential
        """
        return self.label
    
    def isDecrypted(self):
        if self.type_ == 1:
            return True
        return False
    
    def updateNonce(self, nonce):
        """
        Update nonce of the credential
        """
        if self.tag is not None:
            self.nonce = nonce
        return self.nonce
    
    def updateTag(self, tag):
        """
        Update tag of the credential
        """
        self.tag = tag
        return self.tag
    
    def updateToken(self, token):
        """
        Update token of the credential
        """
        #if self.token is not None:
        self.token = token
        return self.token
    
    def updateContent(self, content):
        """
        Update content of the credential
        """
        if self.tag is not None:
            self.content = content
        return self.content
    
    def updateType(self, type_):
        """
        Update type of the credential
        """
        self.type_ = type_
        return self.type_

    def hasKey(self, key):
        """
        Get the key in the object
        """
        if key in self.content:
            return key in self.content
        return None

    def toString(self):
        """
        Transform the string into a base64 type
        return: base64 object
        """
        if self.content is not None and type(self.content) != bytes:
            string_bytes = json.dumps(self.content).encode("ascii")
            base64_bytes = base64.b64encode(string_bytes)
            return base64_bytes
        if self.content is not None and type(self.content) == bytes:
            base64_bytes = base64.b64encode(self.content)
            return base64_bytes
        raise EmptyCredential(self.label)
        
    def getAsJson(self):
        """
        returns object in json format
        """
        if type(self.content) == dict :
            content = json.dumps(self.content).encode('ascii')
            content = base64.b64encode(content).decode("ascii")
        else:
            if self.type_==2:
                content = self.content
            else:
                content = base64.b64encode(self.content.encode("ascii")).decode("ascii")
            
        nonce=None
        if  self.nonce is None:
            nonce = self.nonce
        else:
            nonce = base64.b64encode(self.nonce).decode("ascii")

        tag = None
        if self.tag is not None:            
            tag = base64.b64encode(self.tag).decode("ascii")
        
        return json.dumps({
            "label"      : self.label,
            "content"    : content,
            "uuid"       : self.uuid,
            "nonce"      : nonce,
            "tag"        : tag,
            "token"      : self.token,
            "type_"      : self.type_,
            "creation"   : str(self.creation),
            "expiration" : str(self.expiration)
        }).encode('ascii')
    
    def decrypt(self, tokens):
        if tokens is not None:
            if not isinstance(tokens, list):
                raise UnknownTokenType(tokens)
                
            for token_element in tokens:
                if token_element.getExpiration() <= datetime.now():
                    raise ExpiredToken()
            
            shamir = ShamirImplementation()
            
            return shamir.decryptSecret(tokens, self)
        raise RuntimeError("No tokens given")
    
    @classmethod
    def buildFromJson(cls,json):
        """
        returns representation of the object
        """
        nonce = None
        tag = None
        try:
            content = ast.literal_eval(base64.b64decode(json["content"]).decode('ascii'))
        except Exception as e:
            content = json["content"]
            
        if json["nonce"] is not None:
            nonce = base64.b64decode(json["nonce"])
            
        if json["tag"] is not None:
            tag = base64.b64decode(json["tag"])
                
        return Credential(
            label=json["label"],
            content=content,
            uuid=json["uuid"],
            nonce=nonce,
            tag=tag,
            token=json["token"],
            type_=json["type_"],
            creation=datetime.strptime(json["creation"],'%Y-%m-%d %H:%M:%S'),
            expiration=datetime.strptime(json["expiration"],'%Y-%m-%d %H:%M:%S'),
        )

    def __repr__(self):
        return "Credential[label=%s,uuid=%s,token=%s,type=%s,creation=%s,expiration=%s]" % (self.label, self.uuid, self.token, self.type_, self.creation, self.expiration)
