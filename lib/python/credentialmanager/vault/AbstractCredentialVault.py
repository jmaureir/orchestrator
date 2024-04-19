# Abstract Credential Vault module for Credential Manager

import json
import base64
import ast
import time
from sqlalchemy import create_engine, and_, inspect
from sqlalchemy.sql import func
from sqlalchemy import *
from datetime import datetime
from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15
from ..Credential import *
from ..exceptions import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

class AbstractCredentialVault(BasicLogger):
    
    def query(self, sql):
        e = None
        for retry in range(0,10):
            try:
                result = self.getEngine().execute(sql).fetchall()
                return result
            except Exception as e:
                # retry operation
                time.sleep(10)

        raise RuntimeError("AbstractCredentialVault:Could not execute Query: %s : %s" % (sql,e))
        
    def buildCredentialFromTuple(self, tup, key):
        
        label                    = tup[1]
        b64_encrypted_credential = tup[3]
        uuid_credential          = tup[5]
        nonce_credential         = tup[6]
        tag_credential           = tup[7]
        token_credential         = tup[8]
        creation_credential      = tup[9]
        expiration_credential    = tup[10]
        encrypted_credential     = base64.b64decode(b64_encrypted_credential.encode("ascii"))

        if key.getPrivateKey() is None:
            raise PrivateKeyNotLoaded()

        content_shamir = key.decrypt(encrypted_credential)
        type_credential = 2

        if nonce_credential is not None:

            credential_nonce_encode = nonce_credential.encode("ascii")
            nonce_credential = base64.b64decode(credential_nonce_encode)
            credential_tag_encode = tag_credential.encode("ascii")
            tag_credential = base64.b64decode(credential_tag_encode)

        return Credential(label, content_shamir, uuid_credential, nonce_credential, tag_credential, token_credential, type_credential, creation_credential, expiration_credential)
        
    def __init__(self, owner, conn_str, table_name="credentials"):
        print("credential vault",conn_str)
        self.__engine = create_engine(conn_str,  connect_args={'timeout': 1})
        self.__table_name = table_name
        self.__id = "default"
        self.__owner = owner
        
        #signature credential?
        self.signature_credential = None

        self.hash_message = None
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
        Create a table in the database to be used by the credential
        """
        self.debug("Creating storage")
        engine = self.getEngine()
        metadata = MetaData(engine)
        # Create a table with the appropriate Columns
        self.table = Table(self.__table_name, metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('label', String),
                           Column('key_id', String),
                           Column('credential', String),
                           Column('signature', String),
                           Column('uuid', String),
                           Column('nonce', String),
                           Column('tag', String),
                           Column('token', String),
                           Column('creation', DateTime(timezone=True), server_default=func.now()),
                           Column('expiration', DateTime(timezone=True), onupdate=func.now()),
                           Column('type', Integer),
                           Column('active', Boolean))
        # Implement the creation
        metadata.create_all()

    def getEngine(self):
        """
        Get the engine to be used in the credential, by default is psql
        """
        return self.__engine
    
    def getId(self):
        """
        get the id related to the credential
        """
        return self.__id

    def setId(self, vault_id):
        """
        Set the id of the credential related to the vault
        """
        self.__id = vault_id

    def listCredentials(self, key, active=True):
        """
        List the credentials that are active, if no active is given, by default is True
        """
        key_id = key.getUUID()
        sql = self.table.select().where(and_(
            self.table.columns.active == active,
            self.table.columns.key_id == key_id,
        ))
        results = self.query(sql)
        credential_list = []
        for result in results:
            try:
                credential_list.append(self.buildCredentialFromTuple(result,key))
            except Exception as e:
                raise e
                
        return credential_list

    def existCredential(self, credential):
        """
        Check if the credential exist
        """
        try:
            sql = self.table.select().where(and_(
                self.table.columns.label == credential.getLabel(),
                self.table.columns.active == True
            ))
            result = self.__engine.execute(sql).fetchall()
            if len(result) == 1:
                self.debug("credential label:%s already stored"%(credential.getLabel()))
                return True
            elif len(result) > 1:
                # many credentials match
                raise MultipleCredentialsFound(label)
            else:
                return False
        except Exception as e:
            raise e            

    def getCredential(self, key, label):
        key_id = key.getUUID()

        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.key_id == key_id,
            self.table.columns.active == True
        ))

        result = self.query(sql)
        
        if len(result) == 1:
            try:
                crd = self.buildCredentialFromTuple(result[0], key)
                return crd
            except Exception as e:
                raise e
        elif len(result) > 1:
            # many credentials match
            raise MultipleCredentialsFound(label)
        else:
            raise CredentialNotFound(label)
            
    def encryptCredential(self, credential, destination_key):
        credential_ = credential.content
        label = credential.getLabel()
        uuid = credential.uuid
        nonce = credential.nonce
        tag = credential.tag
        token = credential.token
        type_ = credential.type_
        creation = credential.creation
        expiration = credential.expiration
        serialized_credential = credential.toString()
        encrypted_credential = destination_key.encrypt(serialized_credential)
        b64_encypted_credential = base64.b64encode(encrypted_credential).decode("ascii")
        
        Credential_object = Credential.getFromBase64String(\
        label, uuid, b64_encypted_credential, nonce, tag, token, type_, creation, expiration, True)
        
        return Credential_object

    def storeCredential(self, key, credential):
        """
        Store the credential in the database using sqlAlchemy
        """
        engine = self.getEngine()
        key_id = key.getUUID()
        serialized_credential = credential.toString()
        encrypted_credential = key.encrypt(serialized_credential)
        b64_encypted_credential = base64.b64encode(encrypted_credential).decode("ascii")
        
        
        if credential.nonce is not None:
            credential_uuid = credential.uuid
            credential_nonce = base64.b64encode(credential.nonce).decode("ascii")
            credential_tag = base64.b64encode(credential.tag).decode("ascii")
            credential_token = credential.token
            credential_creation = credential.creation
            credential_expiration = credential.expiration
            credential_type = 3
        else:
            credential_uuid = credential.uuid
            credential_nonce = credential.nonce
            credential_tag = credential.tag
            credential_token = credential.token
            credential_creation = credential.creation
            credential_expiration = credential.expiration
            credential_type = 3
        
        print(credential_token)
        
        if not self.existCredential(credential):
            try:
                sql = self.table.insert().values({
                    "key_id": key_id,
                    "label": credential.getLabel(),
                    "uuid" : credential_uuid,
                    "credential": b64_encypted_credential,
                    "nonce": credential_nonce,
                    "tag": credential_tag,
                    "token": credential_token,
                    "creation": credential_creation,
                    "expiration": credential_expiration,
                    "type": credential_type,
                    "active": True
                })

                result = engine.execute(sql)
                self.debug("credential label:%s has been stored"%(credential.getLabel()))
                if result.rowcount == 1:
                    return True
            except Exception as e:
                raise e

        raise CredentialAlreadyExists(credential.getLabel())
        
    def checkCredentialExpiration(self, label):
        """
        Check the expiration date of the credential
        """
        sql_exist = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.active == True
        ))
        sql = self.table.select().where(and_(
            self.table.columns.expiration <= datetime.now(),
            self.table.columns.label == label,
            self.table.columns.active == True
        ))
        query_exist = self.__engine.execute(sql_exist).fetchall()
        query = self.__engine.execute(sql).fetchall()
        if len(query_exist) == 0:
            raise CredentialNotFound(label)
        if len(query) == 0:
            return True
        else:
            raise ExpiredCredential()
            
    def getCredentialExpirationDate(self, label):
        """
        Get expiration date of credential
        """
        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.active == True
        ))
        query = self.__engine.execute(sql).fetchall()
        if len(query) == 0:
            raise CredentialNotFound(label)
        expiration_date = query[0][9]
        return expiration_date
        
    def setCredentialExpiration(self, label, date):
        """
        Set the expiration date of the credential
        """
        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.active == True
        ))
        query = self.__engine.execute(sql).fetchall()
        if len(query) == 0:
            raise CredentialNotFound(label)
        exp_date = datetime.strptime(date.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
        # Create "Session" class and session
        Session = sessionmaker(bind=self.__engine)
        session = Session()
        try:
            # Update field expiration
            session.query(self.table).filter(self.table.columns.label == label, self.table.columns.active == True).update({"expiration": exp_date})
            session.commit()
        finally:
            session.close()
    
    def storeSignature(self, credential, key_1):
        """
        Create Signature on the credential, it will be store in signature credential using private key
        """
        if credential.type_ == 1:
            private = key_1.getPrivateKey()
            serialized_credential = credential.toString()
            hash_msg = SHA256.new(serialized_credential)
            self.hash_message = hash_msg
            signer = pkcs1_15.new(private)
            self.signature_credential = signer.sign(hash_msg)

            # Store signature on database
            label = credential.getLabel()
            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.active == True
            ))
            # Create "Session" class and session
            Session = sessionmaker(bind=self.__engine)
            session = Session()
            try:
                # Update field Signature
                session.query(self.table).filter(self.table.columns.label == label, self.table.columns.active == True).update({"signature": self.signature_credential})
                session.commit()
            finally:
                session.close()
        else:
            raise AlreadyEncrypted("Credential", credential.getLabel())

    def checkValiditySignature(self, credential, key):
        """
        Check the validity of the credential using the signature credential already store with the original credential
        this will be used usin the public key
        """
        if credential.type_ == 1:
            public_key = key.getPublicKey()
            serialized_credential = credential.toString()
            hash_msg = SHA256.new(serialized_credential)
            self.hash_message = hash_msg

            # Retrieve signature on database
            label = credential.getLabel()

            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.active == True
            ))
            result = self.__engine.execute(sql).fetchall()
            try:
                pkcs1_15.new(public_key).verify(self.hash_message,result[0][4])
                return True
            except (ValueError, TypeError):
                raise hasChanged("Credential")
        else:
            raise AlreadyEncrypted("Credential", credential.getLabel())

    def __repr__(self):
        return "%s" % type(self)
