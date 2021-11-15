

from bupacl.credentialmanager.CredentialManager import CredentialManager
from bupacl.credentialmanager.KeyChain import KeyChain
from bupacl.credentialmanager.EncryptionKey import *
from bupacl.credentialmanager.Credential import Credential
from ..loggers import BasicLogger
import jsonpickle


class OrchCredentialManager:
    def __init__(self, db_conn_str="sqlite:///orchestrator.sqlite"):
        self.orchkey = None
        self.db_conn_str = db_conn_str
        self.key_manager = None
        self.credential_manager = None
        self.logger = BasicLogger("OrchCredentialManager")
        # starting the keychain
        if self.key_manager is None:
            try:
                print("checking keychain...")
                self.key_manager = KeyChain(self.db_conn_str)
            except Exception as e:
                print("%s, instance could not be generated..."%(e))     
        # orchestrator validation
        try:
            self.orchkey = EncryptionKey.load("orchestrator")
            print("recovered master key for the orchestrator")
            status_key = "ok"

        except LocalKeyNotFound as e:
            try:
                print("Master key don't found, generating keys...")
                self.orchkey = EncryptionKey("orchestrator")
                self.orchkey.save()
                status_key = "ok"
                print("successfully created master key")
            except Exception as e:
                status_key = "error"
                print(e)
        except Exception as e:
            status_key = "error"
            raise RuntimeError("instance could not be generated...")
        # start the credential manager
        if status_key == "ok":
            if self.credential_manager is None:
                try:
                    print("checking credential manager...")
                    self.credential_manager = CredentialManager(self.orchkey, self.db_conn_str)
                except Exception as e:
                    print("%s, instance could not be generated..."%(e))
            
            
    def putKey(self, key, passphrase=None):
        """Store public key on keychain"""
        try:
            self.key_manager.store(key, passphrase)
        except Exception as e:
            raise e
    
        
    def getKey(self, label, passphrase=None):
        """ get key form keychain """
        key = None
        try:
            key = self.key_manager.retrieve(label, passphrase)
            key.__class__ = EncryptionKey
        except Exception as e:
            raise e
        return key
        
        
    def getKeyList(self, active=True):
        """list of keys stored on database"""
        try:
            return self.key_manager.getKeyList(active)
        except Exception as e:
            raise e
        
    
    def keyExpiration(self, key):  
        """Expiration date of a key is verified"""
        try:
            return self.key_manager.checkKeyExpiration(key)
        except Exception as e:
            return False
    
    
    def setKeyExpiration(self, key, date):
        """Set expiration date of a key"""
        try:
            self.credential_manager.setKeyExpiration(key, date)
        except Exception as e:
            raise e
            
            
    def getKeyExpirationDate(self, key):
        """Get expiration date of a key"""
        date = None
        try:
            date = self.key_manager.getKeyExpirationDate(key)
        except Exception as e:
            raise e
        return date
            
            
    def getCredentialList(self, active=True):
        """list of credential stored on database"""
        try:
            return self.credential_manager.getCredentialList(active)
        except Exception as e:
            raise e
            
            
    def credentialExpiration(self, label):   
        """Expiration date of credential is verified"""
        try:
            return self.credential_manager.checkCredentialExpiration(label)
        except Exception as e:
            return False
            
            
    def setCredentialExpiration(self, label, date):
        """Set expiration date of a credential"""
        try:
            self.credential_manager.setCredentialExpiration(label, date)
        except Exception as e:
            raise e
    
    
    def getCredentialExpirationDate(self, label):
        """Get expiration date of a credential"""
        date = None
        try:
            date = self.credential_manager.getCredentialExpirationDate(label)
        except Exception as e:
            raise e
        return date
        
        
    def signCredential(self, credential, key=None):
        """Sing Credential of current instance and key"""
        if key is None:
            key = self.orchkey
        self.credential_manager.signCredential(credential, key)
        print("Credential has been signed")
        
    
    def putCredential(self, credential, n_unlock=2, shared_users=4):
        """Register pipelines credentials"""
        try:
            self.credential_manager.store(credential, n_unlock, shared_users)
        except CredentialAlreadyExists:
            print("%s already exists"%(credential.getLabel()))
        except Exception as e:
            raise e
        
    
    def getCredential(self, label, decrypt=True, token=None, whom=None):
        """credential is retrieved from credential manager"""
        credential= None
        try:
            credential= self.credential_manager.retrieve(label, decrypt, token, whom)
            credential.__class__ = Credential
            print("Credential has been retrieve")
        except Exception as e:
            raise e
        return credential
    
    
    def verifyCredential(self, credential):
        """Verify Credential of current instance and key"""
        try:
            return self.credential_manager.verifyCredential(credential, self.orchkey)
        except Exception as e:
            raise e
        
    def encryptCredential(self, credential, recipient_key):
        """Encrypt credential with recipient key"""
        credential_encrypted = self.credential_manager.encryptCredential(credential, recipient_key)
        credential_encrypted.__class__ = Credential
        return credential_encrypted
    
    def createToken(self, credential, n_unlock=2, shared_users=4):
        """Decrypt credential encoded with Shamir"""
        try:
            return self.credential_manager.createToken(credential, n_unlock, shared_users)
        except Exception as e:
            raise e
        
        
    def putToken(self, credential, token_list):
        """Stores generated token for credential on database"""
        try:
            return self.credential_manager.storeToken(credential, token_list)    
        except Exception as e:
            raise e
        
    def registerToken(self, token_list):
        """register of retrieved tokens (strings) in the orchestrator"""
        try:
            for token in token_list:
                b64_token = base64.b64decode(token)
                token_decode = b64_token.decode('ascii')                             
                token_object = jsonpickle.loads(token_decode)
                token_object= token_object[0]
                credential = self.getCredential(token_object.getLabel(), decrypt=False, token=None, whom=None)
                self.credential_manager.storeReceivedToken(credential, token)
        except Exception as e:
            raise e
    
    def getToken(self, label=None, whom=None, active=True):
        """Retrieve token stored on database for credential"""
        try:
            return self.credential_manager.retrieveTokens(label, whom, active)
        except Exception as e:
            raise e
    
    
    def assignToken(self, whom, label, exp_date, comment=""):
        """ Assign inactive tokens to a process """
        try:
            self.credential_manager.assignToken(whom, label, exp_date, comment)
        except Exception as e:
            raise e
    
    
    def getAssignedToken(self, label=None, whom=None):
        """ Retrieve assigned tokens to share via string """
        try:
            return self.credential_manager.getAssignedToken(label, whom)
        except Exception as e:
            raise e
    
    
    def signToken(self, label, whom):
        """Sign credential with key from credential manager"""
        try:
            return self.credential_manager.signToken(label, whom)
        except Exception as e:
            raise e
    
    
    def verifyToken(self, token):
        """
        Check if signed token has been modified
        """
        try:
            return self.credential_manager.verifyToken(token)
        except Exception as e:
            raise e 
    
    
    def getTokenList(self, active=False):
        """Get list of tokens form vault"""
        try:
            return self.credential_manager.getTokenList(active)
        except Exception as e:
            raise e
    
    
    def getTokenExpirationDate(self, label, whom):
        """Get Date expiration on the related token"""
        try:
            return self.credential_manager.getTokenExpirationDate(label, whom)
        except Exception as e:
            raise e
    
    
    def tokenExpiration(self, label, whom):
        """Check expiration of the token related to the user"""
        try:
            return self.credential_manager.checkTokenExpiration(label, whom)
        except Exception as e:
            return False
        
        
    def getPublicKey(self):
        """Public key of the orchestrator is obtained"""
        try:
            return EncryptionKey.importPublicKey(self.orchkey.exportPublicKey())
        except Exception as e:
            raise e
            
    def getRegisterCredential(self, label, token=None, whom=None):
        """obtains the credential registered with the orchestrator"""
        token = self.getToken(label=label, whom=whom, active=True)
        credential = self.getCredential(label=label,decrypt=True,token=token, whom=whom)
        return credential
    
    def checkProcessExpiration(self, process_name):
        """Verifies the expiration of credentials and tokens of a pipeline"""
        flag_credential_expired = False
        flag_token_expired = False
        expired_credential=[]
        expired_token=[]
        whom = process_name
        try:
            token_list=self.getToken(label=None, whom=whom, active=True)
            for token in token_list:
                label = token.getLabel()
                status_credential= self.credentialExpiration(label)
                status_token= self.tokenExpiration(label, whom)
                if status_credential is False:
                    flag_credential_expired = True
                    expired_credential.append(label)
                if status_token is False:
                    flag_token_expired = True
                    expired_token.append(label)
            if flag_credential_expired is True and flag_token_expired is False:
                self.logger.info("credentials with label:%s has been expired" % (expired_credential))
                return False
            elif flag_credential_expired is False and flag_token_expired is True:
                self.logger.info("Tokens with label:%s for process: %s has been expired" % (expired_token, whom))
                return False
            elif flag_credential_expired is True and flag_token_expired is True:
                self.logger.info("credentials with label:%s and Tokens with label:%s for process:%s has been expired"\
                    % (expired_credential, expired_token, whom))
                return False
            elif flag_credential_expired is False and flag_token_expired is False:
                 return True
        except TokenNotFound as e:
            self.logger.info("there is no registered token for this pipeline")
            return "TokenNotFound"
        except Exception as e:
            raise e
