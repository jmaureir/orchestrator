# Abstract Key Vault module for Credential Manager

import base64
from sqlalchemy import create_engine, and_, inspect
from sqlalchemy.sql import func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import *
from datetime import datetime
from ..exceptions import *
from ..RSAImplementation import *
from ..EncryptionKey import *
from ..ProtectedEncryptionKey import *
import pandas as pd
from sqlalchemy.pool import StaticPool

class AbstractKeyVault(BasicLogger):
    """
    Abstract Class for the KeyChain
    """

    def __init__(self, owner, conn_str, table_name="key_chain"):
        self.__engine = create_engine(conn_str, connect_args={'timeout': 1})
        self.__table_name = table_name
        self.__id = "default"
        self.__owner = owner
        self.signature_key = None
        BasicLogger.__init__(self,self.__class__.__name__)
        if not self.initialized():
            self.create()

    def initialized(self):
        engine = self.getEngine()
        ins = inspect(engine)
        if not ins.has_table(self.__table_name):
            self.debug("table %s does not exists" % self.__table_name)
            return False

        metadata = MetaData(engine)
        self.table = Table(self.__table_name, metadata, autoload=True, autoload_with=engine)

        return True

    def create(self):
        """
        Create the Table with the neccesary attributes and types  using SqlAlchemy to store Keys
        """
        self.debug("Creating storage for Keychain")
        engine = self.getEngine()
        metadata = MetaData(engine)
        # Create a table with the appropriate Columns
        self.table = Table(self.__table_name, metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('label', String),
                           Column('uuid', String),
                           Column('key', String),
                           Column('creation', DateTime(timezone=True), server_default=func.now()),
                           Column('expiration', DateTime(timezone=True), onupdate=func.now()),
                           Column('active', Boolean),
                           Column('passphrase', Boolean))
        # Implement the creation
        metadata.create_all()


    def getEngine(self):
        """
        Get the engine that's will be use to manage the keys
        return: a engine object
        """
        return self.__engine


    def getId(self):
        """
        Get the Id of the Key
        return: an Id
        """
        return self.__id


    def setId(self, vault_id):
        """
        Set the Id of the Key from the vault
        param: vault_id, the id of the vault that contains the Key
        """
        self.__id = vault_id


    def checkKeyExpiration(self, key):
        """
        Check the expiration date of the key
        param: key , the key to be check
        """
        key_id = key.getUUID()
        sql_exist = self.table.select().where(and_(
            self.table.columns.uuid == key_id
        ))
        sql = self.table.select().where(and_(
            self.table.columns.expiration <= datetime.now(),
            self.table.columns.uuid == key_id
        ))
        query_exist = self.__engine.execute(sql_exist).fetchall()
        query = self.__engine.execute(sql).fetchall()
        if len(query_exist) == 0:
            raise KeyNotFound(key.getLabel())
        if len(query) == 0:
            return True
        else:
            raise ExpiredKey()
    
    
    def getKeyExpirationDate(self, key):
        """
        Get expiration date of the key
        """
        key_id = key.getUUID()
        sql = self.table.select().where(and_(
            self.table.columns.uuid == key_id
        ))
        query = self.__engine.execute(sql).fetchall()
        if len(query) == 0:
            raise KeyNotFound(key.getLabel())
        expiration_date = query[0][5]
        return expiration_date
            
    def setKeyExpiration(self, key, date):
        """
        Set the expiration date of the key
        """
        key_id = key.getUUID()
        sql = self.table.select().where(and_(
            self.table.columns.uuid == key_id
        ))
        query = self.__engine.execute(sql).fetchall()
        if len(query) == 0:
            raise KeyNotFound(key.getLabel())
        exp_date = datetime.strptime(date.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
        # Create "Session" class and session
        Session = sessionmaker(bind=self.__engine)
        session = Session()
        try:
            # Update field expiration
            session.query(self.table).filter(self.table.columns.uuid == key_id).update({"expiration": exp_date})
            session.commit()
        finally:
            session.close()

    def listKeys(self, active=True):
        """
        List the keys that are active
        return: query that could be manage with a dataframe
        """
        try:
            sql = self.table.select().where(and_(
                self.table.columns.active == active,
            ))
            results = self.__engine.execute(sql).fetchall()
            key_list = []
            for result in results:
                label = result[1]
                uuid = result[2]
                creation = result[4]
                expiration = result[5]
                b64_exp_key = result[3]
                b64_key_bytes = b64_exp_key.encode("ascii")
                exp_key = base64.b64decode(b64_key_bytes).decode("ascii")
                try:
                    key_content = RSA.importKey(exp_key, passphrase=None)
                    key_list.append(EncryptionKey(label, key_id=uuid, key=None, public_key=key_content, creation=creation, expiration=expiration))
                except:
                    key_list.append(ProtectedEncryptionKey(label, creation=creation, expiration=expiration, key_id=uuid, key=b64_exp_key, public_key=None))
            return key_list
        except Exception as e:
            raise e


    def existKey(self, key=None, label=None):
        """
        Check if the key exists or not, if not it's going to raise an exception
        param: key, the key to be check
               label: the label of the key
        return: True if the key exists
        """
        try:
            sql = None
            if label is None and key is not None:
                label = key.getLabel()
            elif label is None and key is None:
                raise RuntimeError("getKey: key or label must be provided")

            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.active == True
            ))
            result = self.__engine.execute(sql).fetchall()
            if len(result) == 1:
                return True
            elif len(result) > 1:
                # many keys match
                return True
            else:
                return False
        except Exception as e:
            raise e


    def getKey(self, label=None, passphrase=None):
        """
        Get the key based on a label and a passphrase
        param: label, the label related to the key
               passphrase, the security passphrase that was attached to the key
        return, an EncryptionKey object with the label, uuid and the content of the key
        """
        sql = None
        if passphrase is None and label is None:
            raise ProtectedEncryptionKey()
        if label is None and passphrase is not None:
            raise LabelNecessary()
        
        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.active == True
        ))
        result = self.__engine.execute(sql).fetchall()
               
        if len(result) == 1:
            # only one key match
            uuid = result[0][2]
            b64_exp_key = result[0][3]
            b64_key_bytes = b64_exp_key.encode("ascii")
            exp_key = base64.b64decode(b64_key_bytes).decode("ascii")
            creation = result[0][4]
            expiration = result[0][5]
            try:
                if passphrase is not None:
                    key_content = RSA.importKey(exp_key, passphrase=passphrase)
                    try:
                        key_content = RSA.importKey(exp_key, passphrase=None)
                    except:
                        return EncryptionKey(label, key_id=uuid, key=key_content, public_key=None, creation=creation, expiration=expiration)
                    return EncryptionKey(label, key_id=uuid, key=None, public_key=key_content, creation=creation, expiration=expiration)
                else:
                    try:
                        key_content = RSA.importKey(exp_key, passphrase=passphrase)
                        check_type= EncryptionKey(label, key_id=uuid, key=key_content, public_key=None, creation=creation, expiration=expiration)
                        message= "check key"
                        message_byte = message.encode("ascii") 
                        encrypted_message= check_type.encrypt(message_byte)
                        decrypted_message_byte = check_type.decrypt(encrypted_message)
                        decrypted_message = decrypted_message_byte.decode("ascii") 
                        if decrypted_message == message:
                            return EncryptionKey(label, key_id=uuid, key=key_content, public_key=None, creation=creation, expiration=expiration)
                    except TypeError as e:
                        return EncryptionKey(label, key_id=uuid, key=None, public_key=key_content, creation=creation, expiration=expiration)
            except:
                raise WrongKeyPassphrase()

        elif len(result) > 1:
            # many keys match
            raise MultipleKeysFound(label)
        else:
            raise KeyNotFound(label)


    def storeKey(self, key, passphrase=None):
        """
        Store keys in the database using SqlAlchemy
        param: key: the key that's looking to be stored
               passphrase, the security passphrase that was attached to the key
        return: True if the store was successful
        """
        engine = self.getEngine()
        if passphrase is None:
            pass_flag = False
        else:
            pass_flag = True
        if not self.existKey(key):
            try:
                # get public and private pem strings
                if key.getPrivateKey() is not None:
                    exported_key = key.getPrivateKey().exportKey("PEM", passphrase, pkcs=1)
                else:
                    if passphrase is None:
                        exported_key = key.getPublicKey().exportKey(format="PEM")
                    else:
                        raise PassphraseNotNecessary()
                    
                b64_exported_key = base64.b64encode(exported_key).decode("ascii")
                sql = self.table.insert().values({
                    "label": key.getLabel(),
                    "uuid": key.getUUID(),
                    "key": b64_exported_key,
                    "creation": key.getCreation(),
                    "expiration": key.getExpiration(),
                    "active": True,
                    "passphrase": pass_flag
                })
                result = engine.execute(sql)
                self.debug("key with label:%s has been stored"%(key.getLabel()))
                if result.rowcount == 1:
                    return True
            except Exception as e:
                raise e

        raise AlreadyExists("Key", key.getLabel())

    def storeSignature(self, owner, key):
        """
        Create Signature on the key, it will be store in key using private key
        """
        private_key = key
        hash_msg = SHA256.new(owner)

        owner = pkcs1_15.new(private_key)
        self.signature_key = owner.sign(hash_msg)

    def checkValiditySignature(self, credential, key):
        """
        Check the validity of the key using the signature
        this will be used usin the public
        """
        public_key = key
        try:
            pkcs1_15.new(public_key).verify(self.signature_credential, credential)
            return True
        except (ValueError, TypeError):
            raise hasChanged("Key")
