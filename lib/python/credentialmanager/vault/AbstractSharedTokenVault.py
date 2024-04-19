# Abstract Token Vault module for Credential Manager

import json
import base64
import ast
from datetime import datetime
import jsonpickle
import time
import traceback

from sqlalchemy import create_engine, and_, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.pool import StaticPool

from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15

from ..Credential import *
from ..exceptions import *
from ..ShamirImplementation import *

class AbstractSharedTokenVault(BasicLogger):

    def query(self, sql):
        e = None
        for retry in range(0,10):
            try:
                result = self.getEngine().execute(sql).fetchall() 
                return result
            except Exception as e:
                time.sleep(10)
                self.getEngine().dispose()
                self.__engine = create_engine(self.conn_str, connect_args={'timeout': 1})

        raise RuntimeError("CredentialMananger:Could not execute Query: %s: %s" % (sql,e))

    def __init__(self, owner, conn_str, table_name="shared_token"):
        print("shared token vault:",conn_str)
        self.__shamir = ShamirImplementation()
        self.__engine = create_engine(conn_str, connect_args={'timeout': 1})
        self.__table_name = table_name
        self.conn_str = conn_str
        self.__id = "default"
        self.__owner = owner
        #signature
        self.signature_token = None
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
        engine = self.getEngine()
        metadata = MetaData(engine)
        # Create a table with the appropriate Columns
        self.table = Table(self.__table_name, metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('label', String),
                           Column('key_id', String),
                           Column('whom', String),
                           Column('token', String),
                           Column('comment', String),
                           Column('creation', DateTime(timezone=True), server_default=func.now()),
                           Column('expiration', DateTime(timezone=True), onupdate=func.now()),
                           Column('signature', String),
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

    def listTokens(self, key, active=True):
        """
        List assigned or unassigned tokens
        """
        key_id = key.getUUID()
        sql = self.table.select().where(and_(
            self.table.columns.active == active,
            self.table.columns.key_id == key_id,
        ))
        result = self.query(sql)
        token_list = []
        for token in result:
            encrypted_token = base64.b64decode(token[4].encode("ascii"))
            serialized_token = key.decrypt(encrypted_token)
            str_token = serialized_token
            token_list.append(Token(label=token[1], token=str_token, type_=1, whom=token[3], comment=token[5], creation=token[6], expiration=token[7]))
        return token_list
    
    def checkTokenExpiration(self, key, label, whom):
        """
        Check the expiration date of the credential
        """
        try:
            key_id = key.getUUID()
            if whom is None:
                sql_exist = self.table.select().where(and_(
                    self.table.columns.label == label,
                    self.table.columns.key_id == key_id,
                ))
                sql = self.table.select().where(and_(
                    self.table.columns.label == label,
                    self.table.columns.key_id == key_id,
                    self.table.columns.expiration <= datetime.now(),
                ))
            else:
                sql_exist = self.table.select().where(and_(
                    self.table.columns.label == label,
                    self.table.columns.key_id == key_id,
                    self.table.columns.whom == whom,
                    self.table.columns.active == True
                ))
                sql = self.table.select().where(and_(
                    self.table.columns.label == label,
                    self.table.columns.key_id == key_id,
                    self.table.columns.whom == whom,
                    self.table.columns.expiration <= datetime.now(),
                    self.table.columns.active == True
                ))
            query_exist = self.query(sql_exist)
            query = self.query(sql)
            if len(query_exist) == 0:
                raise TokenNotFound(label)
            if len(query) == 0:
                return True
            else:
                raise ExpiredToken()
        except Exception as e:
            raise e
            
    def getTokenExpirationDate(self, key, label, whom):
        """
        Get expiration date of credential
        """
        try:
            key_id = key.getUUID()
            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.key_id == key_id,
                self.table.columns.whom == whom,
                self.table.columns.active == True
            ))
            query = self.query(sql)
            if len(query) == 0:
                raise CredentialNotFound(label)
            expiration_date = query[0][7]
            return expiration_date
        except Exception as e:
            raise e

    def existTokens(self, key, credential):
        """
        Check if the credential exist
        """
        try:
            key_id = key.getUUID()
            label = credential.getLabel()
            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.key_id == key_id,
            ))
            result = self.query(sql)
            if len(result) >= 1:
                return True
            elif len(result) == 0:
                return False
        except Exception as e:
            raise e
            
    def getTokensByLabel(self, key, label, whom=None, active=True):
        key_id = key.getUUID()

        if whom is None and label is not None:
            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.key_id == key_id,
                self.table.columns.active == active
            ))
        elif label is None and whom is not None: 
            sql = self.table.select().where(and_(
                self.table.columns.whom == whom,
                self.table.columns.key_id == key_id,
                self.table.columns.active == active
            ))
        elif label is not None and whom is not None: 
            sql = self.table.select().where(and_(
                self.table.columns.label == label,
                self.table.columns.key_id == key_id,
                self.table.columns.whom == whom,
                self.table.columns.active == active
            ))

        result = None
        result = self.query(sql)

        if len(result) >= 1:
            tokens= []
            for row in result:
                #tokens match
                if key.getPrivateKey() is not None:
                    encrypted_row = base64.b64decode(row[4].encode("ascii"))
                    serialized_row = key.decrypt(encrypted_row)
                    str_row = serialized_row
                    tokens.append(Token(label=row[1], token=str_row, type_=1, whom=row[3], comment=row[5], creation=row[6], expiration=row[7]))
                else:
                    tokens.append(Token(label=row[1], token=row[4], type_=2, whom=row[3], comment=row[5], creation=row[6], expiration=row[7]))
            #dict_tokens = ast.literal_eval(tokens[0])
            return tokens
        elif len(result) == 0:
            #tokens not found
            raise TokenNotFound(label)
                
    def assignToken(self, key, whom, label, date, comment, serialize=False):
        """
        Set the expiration date of the credential
        """
        key_id = key.getUUID()
        # Se valida la existencia de token asignado (activo)
        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.key_id == key_id,
            self.table.columns.whom == whom,
            self.table.columns.active == True
        ))
        result = self.query(sql)
        if len(result) >= 1:
            raise TokenAlreadyAssigned(label)
        # Se valida la existencia de token disponibles (no activo)
        sql = self.table.select().where(and_(
            self.table.columns.label == label,
            self.table.columns.key_id == key_id,
            self.table.columns.active == False
        ))
        result = self.query(sql)
        if len(result) == 0:
            raise TokenNotAvailable(label)
        
        exp_date = datetime.strptime(date.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
        # Create "Session" class and session
        Session = sessionmaker(bind=self.__engine)
        session = Session()
        try:
            # Update fields whom and expiration
            record = session.query(self.table)\
            .filter(self.table.columns.label == label, self.table.columns.active == False).first()
            session.query(self.table)\
            .filter(self.table.columns.label == label, self.table.columns.active == False, self.table.columns.Id == record[0])\
            .update({"whom": whom,"expiration": exp_date,"comment": comment,"active": True,})
            session.commit()
        finally:
            session.close()
            
        token_lst = self.getTokensByLabel(key, label, whom)
        
        if serialize:
            return [ t.serialize() for t in token_lst]
        
        return token_lst

    def getAssignedToken(self, key, label, whom, serialize=False):
        token = None
        try:
            token_lst = self.getTokensByLabel(key, label, whom)
        except Exception as e:
            raise(e)
            
        if serialize:
            
            return [ token.serialize() for token in token_lst]
        
        return token_lst
    
    def createToken(self, credential, n_unlock, shared_users):
        """
        Encrypt Credential with Shamir and create tokens
        """
        token = self.__shamir.encryptSecret(credential, n_unlock, shared_users)
        if token is False:
            return "token already generated for this credential"
        return token

    def storeToken(self, key, credential, token_list):
        """
        Store tokens on database
        """
        engine = self.getEngine()
        key_id = key.getUUID()
        
        for token in token_list:
            if credential.getLabel() == token.getLabel():
                try:
                    # backwards compatibility for older tokens
                    # fresh created tokens have the share in bytes and asigned tokens have the share as string
                    token_b64 = token.getToken()
                    if isinstance(token_b64,str):
                        token_b64 = token_b64.encode("ascii")

                    encrypted_token = key.encrypt(token_b64)
                    b64_encypted_token = base64.b64encode(encrypted_token).decode("ascii")

                    active = True
                    if token.getExpiration() is None and token.getWhom() is None:
                        active = False

                    sql = self.table.insert().values({
                        "label"      : credential.getLabel(),
                        "key_id"     : key_id,
                        "whom"       : token.getWhom(),
                        "token"      : b64_encypted_token,
                        "comment"    : token.getComment(),
                        "creation"   : token.getCreation(),
                        "expiration" : token.getExpiration(),
                        "active"     : active
                    })
                    result = engine.execute(sql)
                except Exception as e:
                    print(e)                        
                    traceback.print_exc()
                    raise e
            else:
                raise WrongCredential()
            
    def storeReceivedToken(self, key, credential, token):
        """
        Store tokens on database
        """
        engine = self.getEngine()
        key_id = key.getUUID()
        if not self.existTokens(key, credential):
            if credential.getLabel() == token.getLabel():
                if token.getType() == 1:
                    token_ = token.getToken()
                    token_byte = token_.encode("ascii")            
                    encrypted_token = key.encrypt(token_byte)
                    b64_encypted_token = base64.b64encode(encrypted_token).decode("ascii")
                    try:
                        sql = self.table.insert().values({
                            "label": credential.getLabel(),
                            "key_id": key_id,
                            "whom": token.getWhom(),
                            "token": b64_encypted_token,
                            "comment": token.getComment(),
                            "creation"   : token.getCreation(),
                            'expiration': token.getExpiration(),
                            "active": True
                        })

                        result = engine.execute(sql)

                        return True
                    
                    except Exception as e:
                        raise e
                else:
                    raise AlreadyEncrypted("Token", token.getLabel())
            else:
                raise WrongCredential()
        else:            
            raise TokenAlreadyExists(credential.getLabel())
        
    def storeSignature(self, token, key):
        """
        Create Signature for token, it will be store using private key
        """
        private = key.getPrivateKey()
        if token.getType() == 1:
            serialized_token = json.dumps(token.getToken()).encode("ascii")
            hash_msg = SHA256.new(serialized_token)
            #hash_msg = SHA256.new(token.getToken())
            self.hash_message = hash_msg
            signer = pkcs1_15.new(private)
            self.signature_token = signer.sign(hash_msg)        
            # Create "Session" class and session
            Session = sessionmaker(bind=self.__engine)
            session = Session()
            try:
                # Update field Signature
                session.query(self.table).filter(self.table.columns.label == token.getLabel(), self.table.columns.whom == token.getWhom(), self.table.columns.active == True)\
                .update({"signature": self.signature_token})
                session.commit()
            finally:
                session.close()
        else:
            raise AlreadyEncrypted("Token", token.getLabel())

    def checkValiditySignature(self, token, key):
        """
        Check the validity of the token using the signature credential already store with the original token
        this will be used usin the public key
        """
        b64_token = base64.b64decode(token).decode("ascii")
        token_loaded = jsonpickle.loads(b64_token)
        for token_object in token_loaded:
            if token_object.getType() == 1:
                public_key = key.getPublicKey()
                serialized_token = json.dumps(token_object.getToken()).encode("ascii")
                hash_msg = SHA256.new(serialized_token)
                #hash_msg = SHA256.new(token_object.getToken())
                self.hash_message = hash_msg
                # Retrieve signature on database
                sql = self.table.select().where(and_(
                    self.table.columns.label == token_object.getLabel(),
                    self.table.columns.whom == token_object.getWhom(),
                    self.table.columns.active == True
                ))
                result = self.query(sql)
                try:
                    pkcs1_15.new(public_key).verify(self.hash_message,result[0][8])
                    return True
                except (ValueError, TypeError):
                    raise hasChanged("Token")
            else:
                raise AlreadyEncrypted("Token", token.getLabel())
    
    def __repr__(self):
        return "%s" % type(self)
