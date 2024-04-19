# Encryption Key module for Credential Manager

import os
import uuid
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .exceptions import *
from .RSAImplementation import *
from .loggers import BasicLogger
from .SerializedObject import SerializedObject


class EncryptionKey(BasicLogger,SerializedObject):
    """
    Encryption key manage the encryptions and decryptions of the credential manager as a whole
    EncryptionKey
    Represents a unique RSA Key
    """
    keychain_path = "./.credentials"
    def __init__(self, label, key_id=None, key=None, public_key=None, creation=None, expiration=None):
        self.__rsa = RSAImplementation()
        if creation is None:
            creation = datetime.now()
            creation = creation.replace(microsecond=0)
        if expiration is None:
            expiration = creation + relativedelta(days=365)
        BasicLogger.__init__(self,self.__class__.__name__)
        SerializedObject.__init__(self)
        if key_id is None and key is None:
            self.__key = self.__rsa.generate()
            self.__uuid = str(uuid.uuid4())
            self.__label = label
            self.__pubkey = self.__key.publickey()
            self.__creation = creation
            self.__expiration = expiration
        else:
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

    def getPublicKey(self):
        """
        Get the public key that's going to be used for encryption
        """
        if self.__key is not None:
            return self.__key.publickey()
        else:
             return self.__pubkey

    def getPrivateKey(self):
        """
        Get the private key that's going to be used for decrypt
        """
        return self.__key
       
    def getCreation(self):
        """
        Get the label related to the key
        """
        return self.__creation
    
    def getExpiration(self):
        """
        Get the label related to the key
        """
        return self.__expiration

    def encodeKey(self):
        if self.__key is not None:
            exported_key = self.__key.exportKey("PEM", None, pkcs=1)
            self.__key   = base64.b64encode(exported_key).decode("ascii")  
        if self.__pubkey is not None:
            exported_key = self.__pubkey.exportKey("PEM", None, pkcs=1)
            self.__pubkey   = base64.b64encode(exported_key).decode("ascii") 
        
    def decodeKey(self):
        if self.__key is not None:
            b64_private_bytes = self.__key.encode("ascii")
            private_key = base64.b64decode(b64_private_bytes).decode("ascii")
            self.__key = RSA.importKey(private_key, None)
        if self.__pubkey is not None:
            b64_public_bytes = self.__pubkey.encode("ascii")
            public_key = base64.b64decode(b64_public_bytes).decode("ascii")
            self.__pubkey = RSA.importKey(public_key)  

    @classmethod
    def load(cls, label, passphrase=None):
        """
        Loads private and public key  (!load only private key for the inicial key generated!)
        param: passphrase, the passphrase to secure the key
               label, the label related to the key
        """
        private_key_file = "%s/%s_private.key" % (cls.keychain_path, label)
        public_key_file = "%s/%s_public.key" % (cls.keychain_path, label)
        os.makedirs(cls.keychain_path, exist_ok=True)
        if os.path.exists(private_key_file):
            try:
                with open(private_key_file, 'r') as file:
                    dict_key = json.load(file)
                    key_label = dict_key["label"]
                    uuid = dict_key["uuid"]
                    b64_private_key = dict_key["private_key"]
                    b64_private_bytes = b64_private_key.encode("ascii")
                    private_key = base64.b64decode(b64_private_bytes).decode("ascii")
                    creation = datetime.strptime(dict_key['creation'],'%Y-%m-%d %H:%M:%S')
                    expiration = datetime.strptime(dict_key['expiration'],'%Y-%m-%d %H:%M:%S')
                    try:
                        key = RSA.importKey(private_key, passphrase=passphrase)
                    except ValueError:
                        raise WrongKeyPassphrase()
                    return EncryptionKey(key_label, key_id=uuid, key=key, public_key=None, creation=creation, expiration=expiration)
            except Exception as e:
                raise (e)
        elif os.path.exists(public_key_file):
            try:
                with open(public_key_file, 'r') as file:
                    dict_key = json.load(file)
                    key_label = dict_key["label"]
                    uuid = dict_key["uuid"]           
                    b64_public_key = dict_key["public_key"]
                    b64_public_bytes = b64_public_key.encode("ascii")
                    public_key = base64.b64decode(b64_public_bytes).decode("ascii")
                    pubkey = RSA.importKey(public_key) 
                    creation = datetime.strptime(dict_key['creation'],'%Y-%m-%d %H:%M:%S')
                    expiration = datetime.strptime(dict_key['expiration'],'%Y-%m-%d %H:%M:%S')
                    return EncryptionKey(key_label, key_id=uuid, key=None, public_key=pubkey, creation=creation, expiration=expiration)
            except Exception as e:
                raise (e)
        else:
            raise (LocalKeyNotFound(label))
    
    def save(self, passphrase=None):
        """
        Saves the encrypted data into the credentials file
        param: passphrase, the passphrase for the protection of the key
        """
        os.makedirs(self.keychain_path, exist_ok=True)
        private_key_file = "%s/%s_private.key" % (self.keychain_path, self.__label)
        public_key_file = "%s/%s_public.key" % (self.keychain_path, self.__label)
        if self.__key is not None:
            if not os.path.exists(private_key_file):
                # Create private key
                exported_key = self.__key.exportKey("PEM", passphrase, pkcs=1)
                b64_exported_key = base64.b64encode(exported_key).decode("ascii") 
                with open(private_key_file, 'w+', encoding='utf-8') as pk:
                    dict_key = {}
                    dict_key["label"] = self.__label
                    dict_key["uuid"] = self.__uuid
                    dict_key["private_key"] = b64_exported_key
                    dict_key["creation"] = str(self.__creation)
                    dict_key["expiration"] = str(self.__expiration)
                    json.dump(dict_key, pk)
            else:
                raise (KeyAlreadyExists(self.__label))
        else:
            if not os.path.exists(public_key_file):
                # Create public key
                public_key = self.__pubkey
                exported_public_key = public_key.exportKey(format="PEM")
                b64_exported_public_key = base64.b64encode(exported_public_key).decode("ascii")
                with open(public_key_file, 'w+', encoding='utf-8') as puk:
                    dict_key = {}
                    dict_key["label"] = self.__label
                    dict_key["uuid"] = self.__uuid
                    dict_key["public_key"] = b64_exported_public_key
                    dict_key["creation"] = str(self.__creation)
                    dict_key["expiration"] = str(self.__expiration)
                    json.dump(dict_key, puk)
            else:
                raise (KeyAlreadyExists(self.__label))
            
    def exportPublicKey(self):
        """
        Export public key information
        """
        try:
            public_key = self.__key.publickey()
            exported_public_key = public_key.exportKey(format="PEM")
            b64_exported_public_key = base64.b64encode(exported_public_key).decode("ascii")
            dict_key = {}
            dict_key["label"] = self.__label
            dict_key["uuid"] = self.__uuid
            dict_key["public_key"] = b64_exported_public_key
            dict_key["creation"] = str(self.__creation)
            dict_key["expiration"] = str(self.__expiration)
            dict_key_encode = json.dumps(dict_key).encode("ascii")
            b64_dict_key = base64.b64encode(dict_key_encode).decode("ascii") 
            return b64_dict_key
        except Exception as e:
            raise (e)
        
    def importPublicKey(b64_key):
        """
        Import public key information and return ecryption key object
        """
        try:
            dict_key_decoded = base64.b64decode(b64_key).decode("ascii")
            dict_key = json.loads(dict_key_decoded)
            key_label = dict_key["label"]
            uuid = dict_key["uuid"]
            b64_public_key = dict_key["public_key"]
            b64_public_bytes = b64_public_key.encode("ascii")
            public_key = base64.b64decode(b64_public_bytes).decode("ascii")
            pubkey = RSA.importKey(public_key)
            creation = datetime.strptime(dict_key['creation'],'%Y-%m-%d %H:%M:%S')
            expiration = datetime.strptime(dict_key['expiration'],'%Y-%m-%d %H:%M:%S')
            return EncryptionKey(key_label, key_id=uuid, key=None, public_key=pubkey, creation=creation, expiration=expiration)
        except Exception as e:
            raise (e)
        
    def destroy(self):
        """
        Overwrite the key file if exists, then destroy it
        """
        os.makedirs(self.keychain_path, exist_ok=True)

        if self.__key is not None:
            key_file = "%s/%s.key" % (self.keychain_path, self.__label)
            if os.path.exists(key_file):
                try:
                    os.remove(key_file)
                except Exception as e:
                    raise (e)
            else:
                raise (LocalKeyNotFound(self.__label))
        else:
            raise (KeyNotLoaded())

    def encrypt(self, msg):
        """
        This method encrypt data based on a public key
        :param public_key:
        :param msg:
        :return: the data encrypted
        """
        if isinstance(msg, (bytes, bytearray)):
            encrypted = self.__rsa.encrypt(msg, key=self)
            return encrypted
        else:
            return RuntimeError("The msg is not a byte string")

    def decrypt(self, encrypted):
        """
        This method decrypts the data based on a private key
        :param private_key: The private key required for decryption
        :param encrypted: The encrypted data that's looking to be decrypted
        :return: the data decrypted
        """
        msg = self.__rsa.decrypt(encrypted, key=self)
        return msg
    
    def getAsJson(self):
        """
        returns object in json format
        """
        self.encodeKey()
        json_key = json.dumps({
            "label"      : self.__label,
            "key_id"     : self.__uuid,
            "key"        : self.__key,
            "public_key" : self.__pubkey,
            "creation"   : str(self.__creation),
            "expiration" : str(self.__expiration)
        }).encode('ascii')
        self.decodeKey()
        return json_key

    @classmethod
    def buildFromJson(cls,json):
        """
        returns representation of the object
        """
        private_key= None
        public_key= None
        if json["key"] is not None:
            b64_private_bytes = json["key"].encode("ascii")
            decode_key = base64.b64decode(b64_private_bytes).decode("ascii")
            private_key = RSA.importKey(decode_key, None)
        if json["public_key"] is not None:
            b64_public_bytes = json["public_key"].encode("ascii")
            decode_key = base64.b64decode(b64_public_bytes).decode("ascii")
            public_key = RSA.importKey(decode_key)
        return EncryptionKey(
            label=json["label"],
            key_id=json["key_id"],
            key=private_key,
            public_key=public_key,
            creation=datetime.strptime(json["creation"],'%Y-%m-%d %H:%M:%S'),
            expiration=datetime.strptime(json["expiration"],'%Y-%m-%d %H:%M:%S')
        )

    def __repr__(self):
        return "EncryptionKey[label=%s,id=%s,key=%s,pubkey=%s,creation=%s,expiration=%s]" %\
            (self.__label, self.__uuid, self.__key, self.__pubkey, self.__creation, self.__expiration)
