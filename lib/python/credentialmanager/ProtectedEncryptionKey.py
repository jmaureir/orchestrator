# ProtectedEncryptionKey Key module for Credential Manager

import os
import uuid
import json
from .exceptions import *
from .RSAImplementation import *
from .loggers import BasicLogger
from .EncryptionKey import *


class ProtectedEncryptionKey(BasicLogger):
    """
    ProtectedEncryptionKey key manage the decryptions to get... 
    """
    def __init__(self, label, creation, expiration, key_id=None, key=None, public_key=None):
        self.__rsa = RSAImplementation()
        BasicLogger.__init__(self,self.__class__.__name__)
        self.__label = label
        self.__uuid = key_id
        self.__key = key
        self.__pubkey = public_key
        self.__creation = creation
        self.__expiration = expiration

    def getUUID(self):
        """
        Get the uuuid from the instance
        """
        return self.__uuid

    def getLabel(self):
        """
        Get the label related to the key
        """
        return self.__label


    def getPrivateKey(self):
        """
        Get the private key that's going to be used for decrypt
        """
        return self.__key

    def decryptKey(self, passphrase=None):
        """
        Loads private and public key  (!load only private key for the inicial key generated!)
        param: passphrase, the passphrase to secure the key
               label, the label related to the key
        """
        try:
            b64_private_key = self.__key
            b64_private_bytes = b64_private_key.encode("ascii")
            private_key = base64.b64decode(b64_private_bytes).decode("ascii")
            try:
                key = RSA.importKey(private_key, passphrase=passphrase)
            except ValueError:
                raise WrongKeyPassphrase()
            return EncryptionKey(self.__label, key_id=self.__uuid, key=key, public_key=None, creation=self.__creation, expiration=self.__expiration)
        except Exception as e:
            raise (e)

    def __repr__(self):
        return "ProtectedEncryptionKey[label=%s,id=%s,key=%s,pubkey=%s,creation=%s,expiration=%s]" %\
            (self.__label, self.__uuid, self.__key, self.__pubkey, self.__creation, self.__expiration)
