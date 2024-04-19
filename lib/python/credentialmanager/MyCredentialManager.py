# My Credential Manager for personal credential administration
import os
import getpass
import pandas as pd

import parsedatetime
from datetime import datetime
from dotenv import load_dotenv

from .KeyChain import *
from .CredentialManager import *
from .Credential import *
from .EncryptionKey import *
from .Token import *
from .exceptions import *
from .loggers import BasicLogger

class MyCredentialManager(BasicLogger):
    """
    Credentials, Keys and tokens administration made easy
    """
    
    def handle_exception(self, error=None, kind=RuntimeError):
        if error is None:
            error = str(kind)
        if self.raise_exceptions:
            raise(kind(error))
        else:
            print("%s (%s)" % (error,kind))
    
    def __init__(self, keychain_path = None, credential_vault_path = None, key_label="my-master-key", exceptions=False):
        BasicLogger.__init__(self,self.__class__.__name__)
        
        load_dotenv()
        
        passphrase = None
        if "MYCREDENTIALMANAGER_PWD" in os.environ:
            print("using key passprhase from env")
            passphrase  = os.environ["MYCREDENTIALMANAGER_PWD"]
        else:
            print("Enter your key passphrase")
            passphrase = getpass.getpass()
        
        self.raise_exceptions = exceptions
        
        HOME = os.environ["HOME"]
        vault_name = "credentialVault"
        if "CREDENTIALVAULT_NAME" in os.environ:
            vault_name = os.environ["CREDENTIALVAULT_NAME"]
        
        if keychain_path == None:
            keychain_path = f"{HOME}/.credentials"
        
        if credential_vault_path == None:
            credential_vault_path       = f"sqlite:///{HOME}/.credentials/{vault_name}.sqlite"
        
        EncryptionKey.keychain_path = keychain_path
        self._crd_vault_path = credential_vault_path
        self._key = None
        self._keyChain = None
        # manager key creation.
        # this key is local and used to encrypt the credential locally in your credential vault
        # the key is lodaded from file, and when it does not exists, then it is created and saved
        try:
            self._key = EncryptionKey.load(key_label, passphrase=passphrase)
        except LocalKeyNotFound as e:
            print("creating a new key for this credential manager")
            self._key = EncryptionKey(key_label)
            self._key.save(passphrase=passphrase)
        except WrongKeyPassphrase as e:
            self.handle_exception(e)
            return
        except Exception as e:
            self.handle_exception(e)
            return
            
        self.info("Key loaded")
        # credential manager associated to my_master_key and with vault path at credential_vault_path
        self.credential_manager = CredentialManager(self._key, conn_db = self._crd_vault_path)
        # KeyChain
        self._keyChain = KeyChain(conn_db = self._crd_vault_path)    
    
    def store(self, credential, num_tokens=4, tokens_unlock=2, verbose=False):
        try:
            # store the credential in the local credential vault
            # here is when the credential is encrypted with the master key and the
            # shamir security layer is created
            if verbose:
                print("storing credential with %d tokens and a %d required tokens to unlock" % (num_tokens,tokens_unlock))
            self.credential_manager.store(credential,n_unlock=tokens_unlock, shared_users=num_tokens)
        except Exception as e:
            self.handle_exception(e)
            
    def retrieve(self, label, decrypt=False, token=None, verbose=True, whom=None):
        # we recover the credential only removing the master key encription and keeping the shamir security layer
        # the argument decrypt=False is used to retrieve the credential only with the master key decription
        my_credential = None
        try:
            my_credential = self.credential_manager.retrieve(label, decrypt=decrypt, token=token, whom=whom)
            self.info("Credential with label:%s has been retrieved"%(my_credential.getLabel()))
        except Exception as e:
            self.handle_exception(e)
        
        if my_credential is not None and my_credential.type_==2:
            if verbose:
                print("credential still encrypted since no enough tokens to decrypt")
        
        return my_credential

    def getCredentialsList(self, columns=["Label","uuid","type","creation","expiration"]):
        """
        get credentials as list
        """
        return self.credential_manager.getCredentialList()
    
    def getCredentials(self, columns=["Label","uuid","type","creation","expiration"]):
        """
        get credentials as a dataframe
        """
        c_lst = []

        for c in self.credential_manager.getCredentialList():
            c_lst.append([ c.label,c.uuid, c.nonce, c.tag, c.token, c.type_, c.creation, c.expiration])

        default_cols = ["Label","uuid","nonce","tag","token","type","creation","expiration"]
        if len(columns)>0:
            if not all(item in default_cols for item in columns):
                self.handle_exception(RuntimeError("Unknown column requested: default cols",default_cols))
        else:
            columns = default_cols
        
        df_credentials = pd.DataFrame(c_lst,columns=default_cols)
        return df_credentials[columns]
    
    def getTokenList(self, **kw_args):
        return self.credential_manager.getTokenList(**kw_args)    
    
    def getTokens(self,crd_label,whom=None):
        token = None
        if whom is None:
            try:
                token = self.credential_manager.retrieveTokens(crd_label)
            except Exception as e:
                self.handle_exception(e)
        else:
            try:
                token = self.credential_manager.getAssignedToken(crd_label, whom)
            except Exception as e:
                self.handle_exception(e)
        return token
    
    def getAvailableTokens(self):
        token_lst = self.getTokenList()
        token_df_lst = []
        for token in token_lst:
            token_df_lst.append([token.getLabel(), token.getType(), token.getWhom(), token.getComment(), token.getCreation(), token.getExpiration()])
            
        default_cols = ["Label","Type","Whom","Comment","Creation","Expiration"]
        return pd.DataFrame(token_df_lst, columns=default_cols)
    
    def getAssginedTokens(self):
        token_lst = self.credential_manager.getTokenList(active=True)
        token_df_lst = []
        for token in token_lst:
            token_df_lst.append([token.getLabel(), token.getType(), token.getWhom(), token.getComment(), token.getCreation(), token.getExpiration()])
            
        default_cols = ["Label","Type","Whom","Comment","Creation","Expiration"]
        return pd.DataFrame(token_df_lst, columns=default_cols)
    
    def getPublicKey(self):
        try:
            return self._key.exportPublicKey()
        except Exception as e:
            self.handle_exception(e)
        return None
    
    def assignToken(self, crd_label, whom=None,duration="365 days", comment=None, verbose=True):
        if whom is None:
            whom = self._key.getLabel()
            
        if comment is None:
            print("token assignation comment (required):")
            comment  = input()
        
        # compute expiration date 
        cal = parsedatetime.Calendar()
        time_struct, parse_status = cal.parse(duration)
        expiration_date = datetime(*time_struct[:6])

        asigned_token = None
        if verbose:
            print("assigning token to %s. expires in %s" % (whom,expiration_date))

            try:
                asigned_token = self.credential_manager.assignToken(whom=whom,label=crd_label, exp_date=expiration_date,comment=comment)
            except Exception as e:
                self.handle_exception(e)

        return asigned_token
    
    def export(self, what):
        if isinstance(what,Credential):
            print("exporting credential")
            if what.type_==1:
                self.handle_exception(RuntimeError("could not serialize a decrypted credential"))
            crd_ser = what.serialize()    
            return crd_ser
        
        elif isinstance(what,Token):
            print("exporting token")
            token_ser = what.serialize()
            return token_ser
        else:
            self.handle_exception(RuntimeError("Unknown object. Only accepted Credential or Token"))
            
    def imports(self, what_s):
        
        try:
            # try with credential
            crd = Credential.deserialize(what_s)
            self.store(crd)
            return True
        except Exception as e:
            # try with token
            try:
                token = Token.deserialize(what_s)
                
                crd_label = token.getLabel()
                try:
                    crd4token = self.retrieve(crd_label)
                    print("credential associated to token",crd4token)
                    try:
                        self.credential_manager.storeReceivedToken(crd4token, token)
                        return True
                    except Exception as e:
                        self.handle_exception(e)
                    
                except Exception as e:
                    self.handle_exception(e)
                
            except Exception as e:
                self.handle_exception(e)
        return False
        
    
