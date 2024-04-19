# Credential Manager module for Credential Manager

import ctypes
from .vault.CredentialVault import *
from .vault.TokenVault import *
from .vault.KeyVault import *
from .Credential import *
from .ShamirImplementation import *
from .EncryptionKey import *
from .ProtectedEncryptionKey import ProtectedEncryptionKey
import inspect
import sqlalchemy as sal
import traceback

class CredentialManager(object):
    """
    Credential Manager, the base of this module
    """
    def __init__(self, key, conn_db='sqlite:///./.credentials/localVault.sqlite'):
        # todo: check whether key is an instance of EncryptionKey or not
        self.owner_key = key
        # handle multiple vault types
        self.vaults = {
            "credentials" :{},
            "tokens"      :{},
            "keys"        :{}            
        }
        # only one vault is current for each type
        self.current_vault = {
            "credentials" :None,
            "tokens"      :None,
            "keys"        :None
        }
        self.__shamir = ShamirImplementation()
        self.conn_db = conn_db
        
        # create default credential vault
        self.attachVault(LocalCredentialsVault)
        self.attachVault(LocalSharedTokenVault)
        self.attachVault(LocalKeyVault)

    def attachVault(self, vault_type, **kwargs):
        """
        Attach a vault into the credential manager
        param: vault_type, the vault to be attached
        """
        vault_type_in_scope = inspect.isclass(vault_type)
        if vault_type_in_scope:
            vault = vault_type(self, **kwargs)
            vault_id = vault.getId()
            
            if isinstance(vault, AbstractCredentialVault):
                self.vaults["credentials"][vault_id] = vault
                self.current_vault["credentials"] = vault
            elif isinstance(vault, AbstractSharedTokenVault):
                self.vaults["tokens"][vault_id] = vault
                self.current_vault["tokens"] = vault
            elif isinstance(vault, AbstractKeyVault):
                self.vaults["keys"][vault_id] = vault
                self.current_vault["keys"] = vault
            else:
                raise VaultNotFound(str(vault_type))

        else:
            raise VaultNotFound(str(vault_type))

    def detachVault(self, vault_id):
        """
        Detach a vault from the credential manager
        param: vault_id, the id of the vault to be detach
        """
        
        if vault_id == "default":
            raise RuntimeError("Could not detach default vault")
        
        if vault_id in self.vaults["credentials"]:
            # TODO: verify when vauld_id exists in the dict
            del self.vaults["credentials"][vault_id]            
            self.current_vault = self.vaults["credentials"]["default"]
        elif vault_id in self.vaults["keys"]:
            del self.vaults["keys"][vault_id]            
            self.current_vault = self.vaults["keys"]["default"]
        elif vault_id in self.vaults["tokens"]:
            del self.vaults["tokens"][vault_id]            
            self.current_vault = self.vaults["tokens"]["default"]
        else:
            raise VaultNotFound(vault_id)

    def retrieve(self, label, decrypt=True, token=None, whom=None):
        """
        retrieve the credential from the vault by the owner key and the label
        param: label, the label related to the credential
        return: the credential as a base64 object
        """
        if whom is None:
            whom = self.owner_key.getLabel()
        
        if not isinstance(label, str):
            raise UnknownType(label)
        try:
            credential = self.current_vault["credentials"].getCredential(self.owner_key, label)
            
            if token is not None:
                if not isinstance(token, list):
                    raise UnknownTokenType(token)
                # when credential is decrypted with token the content of the credential is updated
                self.decryptCredential(credential, token, whom)
                if credential.type_ != 1:
                    raise CredentialNotDecrypted()
                    
            elif decrypt is True and token is None:
                self.decryptCredential(credential, token, whom)
                if credential.type_ != 1:
                    raise CredentialNotDecrypted()
                    
            return credential
        except Exception as e:
            raise e
    
    def store(self, credential, n_unlock=2, shared_users=4):
        """
        encrypt with shamir and store credential by the current vault of the owner
        param: credential, the credential to be store
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
        if not isinstance(n_unlock, int):
            raise UnknownType(n_unlock)
        if not isinstance(shared_users, int):
            raise UnknownType(shared_users)
        try:
            if not self.current_vault["credentials"].existCredential(credential):
                if credential.type_ == 1:
                    # when creating the tokens, the credential is encrypted with shamir updating the content of the credential
                    token_list = self.createToken(credential, n_unlock, shared_users)
                    result = self.storeToken(credential, token_list)                    
                    
                self.current_vault["credentials"].storeCredential(self.owner_key, credential)   
                return True
            else:
                raise CredentialAlreadyExists(credential.label)
        except Exception as e:
            raise e
        
    def createToken(self, credential, n_unlock=2, shared_users=4):
        """
        encrypt with shamir and create tokens:
        when creating the tokens, the credential is encrypted with shamir
        updating the content of the credential
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
        if not isinstance(n_unlock, int):
            raise UnknownType(n_unlock)
        if not isinstance(shared_users, int):
            raise UnknownType(shared_users)
        try:
            token_list = self.current_vault["tokens"].createToken(credential, n_unlock, shared_users)
            return token_list
        except Exception as e:
            raise e
    
    def storeToken(self, credential, token_list):
        """
        stores token received by credential owner
        param: token, additional token required to decrypt credential
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
        if not isinstance(token_list, list):
            raise UnknownTokenType(token_list)
        try:
            result = self.current_vault["tokens"].storeToken(self.owner_key, credential, token_list)
            return result
        except Exception as e:
            raise e
            
    def storeReceivedToken(self, credential, token):
        """
        stores token received by credential owner
        param: token, additional token required to decrypt credential
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
            
        token_lst = []
        if not isinstance(token, Token):
            if not isinstance(token,list):
                raise UnknownTokenType(token)
            token_lst = token
        else:
            token_lst = [ token ]
        try:
            r = []
            for token in token_lst:
                result = self.current_vault["tokens"].storeReceivedToken(self.owner_key, credential, token)
                r.append(result)
                
            return r
        except Exception as e:
            raise e
    
    def retrieveTokens(self, label=None, whom=None, active=False):
        """
        Retrieve tokens for credential label on database
        """
        if label is None and whom is None:
            raise ValueError("you must enter at least one parameter")
        if label is not None:
            if not isinstance(label, str):
                raise UnknownType(label)
        if whom is not None:
            if not isinstance(whom, str):
                raise UnknownType(whom)
        try:
            result = self.current_vault["tokens"].getTokensByLabel(self.owner_key, label, whom, active=active)
            return result
        except Exception as e:
            raise e
        
    def assignToken(self, whom, label, exp_date=None, comment="", serialize=False):
        """
        Assigns available tokens
        """
        if exp_date is None:
                    now = datetime.now()
                    exp_date = now + relativedelta(days=365)
        if not isinstance(label, str):
            raise UnknownType(label)
        if not isinstance(whom, str):
            raise UnknownType(whom)
        if not isinstance(exp_date, datetime):
            raise UnknownType(exp_date)
        try:
            if exp_date < datetime.now():
                raise PastExpirationDate()
            date= exp_date            
            assigned_token = self.current_vault["tokens"].assignToken(self.owner_key, whom, label, date, comment, serialize=serialize)
            return assigned_token
        except Exception as e:
            raise e       
        
    def getAssignedToken(self, label:str, whom:str, serialize=False):
        try:
            token_to_send = self.current_vault["tokens"].getAssignedToken(self.owner_key, label, whom, serialize=serialize)
            return token_to_send
        except Exception as e:
            raise e        
            
    def signToken(self, label, whom):
            """
            Sign credential with key from credential manager
            """
            try:
                tokens = self.retrieveTokens(label, whom, True)
                for token in tokens:
                    self.current_vault["tokens"].storeSignature(token, self.owner_key)
            except Exception as e:
                raise e              
                
    def verifyToken(self, token):
            """
            Check if signed token has been modified
            """
            try:
                return self.current_vault["tokens"].checkValiditySignature(token, self.owner_key)
            except Exception as e:
                raise e         
                
    def getTokenList(self, active=False):
        """
        Get Tokens from vault
        active: True by default list assigned token
        active: False list not assigned token
        """
        if not isinstance(active, bool):
            raise UnknownType(active)
        try:
            token_list = self.current_vault["tokens"].listTokens(self.owner_key, active=active)
            return token_list
        except Exception as e:
            raise e
            
    def getTokenExpirationDate(self, label, whom):
        """
        Get Date expiration on the related key
        """
        if not isinstance(label, str):
            raise UnknownType(label)
        if not isinstance(whom, str):
            raise UnknownType(whom)
        try:
            exp_date = self.current_vault["tokens"].getTokenExpirationDate(self.owner_key, label, whom)
            return exp_date
        except Exception as e:
            raise e
            
    def checkTokenExpiration(self, label, whom=None):
        """
        Check expiration of the token related to the user
        """
        if not isinstance(label, str):
            raise UnknownType(label)
        if whom is not None:
            if not isinstance(whom, str):
                raise UnknownType(whom)
        try:
            status = self.current_vault["tokens"].checkTokenExpiration(self.owner_key, label, whom)
            return status
        except Exception as e:
            raise e
    
    def decryptCredential(self, shamir_credential, token, whom=None):
        """
        decrypt credential to see the content 
        param:
        """ 

        if not isinstance(shamir_credential, Credential):
            raise UnknownCredentialType(shamir_credential)
        if token is not None:
            if not isinstance(token, list):
                raise UnknownTokenType(token)
                
        if whom is None:
            whom = self.owner_key.getLabel()
        try:
            if token is None:
                # look for tokens in the local vault
                token = self.retrieveTokens(shamir_credential.getLabel(), whom, active=True)
                status = self.checkTokenExpiration(shamir_credential.getLabel(), whom)                
                if status:
                    # when credential is decrypted with token the content of the credential is updated
                    return self.__shamir.decryptSecret(token, shamir_credential)
            else:
                for token_element in token:
                    if token_element.getExpiration() <= datetime.now():
                        raise ExpiredToken()
                else:
                    # when credential is decrypted with token the content of the credential is updated
                    return self.__shamir.decryptSecret(token, shamir_credential)
                
        except Exception as e:
            raise e
    
    def getCredentialList(self, active=True):
        """
        Get Credential from the vault with the same owner
        param: active, by default is True
        return the credential list of the owner that are active
        """
        if not isinstance(active, bool):
            raise UnknownType(active)
        try:
            return self.current_vault["credentials"].listCredentials(self.owner_key,active=active)
        except Exception as e:
            raise e
    
    def signCredential(self, credential, key):
        """
        Sign credential with key from credential manager
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
        try:
            return self.current_vault["credentials"].storeSignature(credential=credential,key_1=key)
        except Exception as e:
            raise e
            
    def encryptCredential(self, credential, destination_key):
        """
        Encrypt open credential with key given as argument
        """
        if not isinstance(credential, Credential):
            raise UnknownCredentialType(credential)
            
        if not isinstance(destination_key, EncryptionKey):
            raise UnknownKeyType(destination_key)
        try:
            credential_ = self.current_vault["credentials"].encryptCredential(credential, destination_key)
            return credential_
        except Exception as e:
            raise e

    def verifyCredential(self, credential, key):
        """
        Check if signed credential has been modified
        """
        try:
            return self.current_vault["credentials"].checkValiditySignature(credential=credential, key=key)
        except Exception as e:
            raise e
            
    def checkCredentialExpiration(self, label):
        """
        Check the expire on the related credential
        """
        if not isinstance(label, str):
            raise UnknownType(label)
        try:
            return self.current_vault["credentials"].checkCredentialExpiration(label)
        except Exception as e:
            raise e
            
    def setCredentialExpiration(self, label, exp_date):
        """
        Set the expire on the related key
        param:key to configure and expiration date
        """
        if not isinstance(label, str):
            raise UnknownType(label)
        try:
            self.current_vault["credentials"].setCredentialExpiration(label, exp_date)
        except Exception as e:
            raise e
            
    def getCredentialExpirationDate(self, label):
        """
        Get Date expiration on the related key
        """
        if not isinstance(label, str):
            raise UnknownType(label)
        try:
            return self.current_vault["credentials"].getCredentialExpirationDate(label)
        except Exception as e:
            raise e
            
    def setKeyExpiration(self, key, exp_date):
        """
        Set the expire on the related key
        param:key to configure and expiration date
        """
        if not isinstance(key, EncryptionKey):
            raise UnknownKeyType(key)
        try:
            self.current_vault["keys"].setKeyExpiration(key, exp_date)
        except Exception as e:
            raise e
            
    def getKey(self):
        """
        get the Key of the Credential
        """
        return self.owner_key

    def getCurrentCredentialsVault(self):
        """
        get the currentVault of the Credential
        """
        return self.current_vault["credentials"]
    
    def getCurrentTokensVault(self):
        """
        get the currentVault of the Tokens
        """
        return self.current_vault["tokens"]
    
    def getCurrentKeysVault(self):
        """
        get the currentVault of the Keys
        """
        return self.current_vault["keys"]
