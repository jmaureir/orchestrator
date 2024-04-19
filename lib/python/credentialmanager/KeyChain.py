# KeyChain module for Credential Manager

from datetime import datetime
from .vault.KeyVault import *
from .EncryptionKey import *
import inspect


class KeyChain(object):
    """
    Keychain of EncryptionKey
    """

    def __init__(self, conn_db='sqlite:///./.credentials/localVault.sqlite'):
        # handle vault types
        self.vaults = {
            "keys"        :{}            
        }
        # only one vault is current for each type
        self.current_vault = {
            "keys"        :None
        }
        self.conn_db = conn_db
        # create default key vault
        self.attachVault(LocalKeyVault)
        

    def attachVault(self, vault_type, **kwargs):
        """
        Attach a vault into the keychain
        param: vault_type, the vault to be attached
        """
        vault_type_in_scope = inspect.isclass(vault_type)
        if vault_type_in_scope:
            vault = vault_type(self, **kwargs)
            vault_id = vault.getId()
            
            if isinstance(vault, AbstractKeyVault):
                self.vaults["keys"][vault_id] = vault
                self.current_vault["keys"] = vault
            else:
                raise VaultNotFound(str(vault_type))

        else:
            raise VaultNotFound(str(vault_type))

    def detachVault(self, vault_id):
        """
        Detach a vault from keychain
        param: vault_id, the id of the vault to be detach
        """
        
        if vault_id == "default":
            raise RuntimeError("Could not detach default vault")
        
        if vault_id in self.vaults["keys"]:
            del self.vaults["keys"][vault_id]            
            self.current_vault = self.vaults["keys"]["default"]
        else:
            raise VaultNotFound(vault_id)

    def retrieve(self, label, passphrase=None):
        """
        get the Key from the current vault by the label and a security passphrase
        param: passphrase: security passphrase for the key
               label, label of the key
        """
        key = self.current_vault["keys"].getKey(label=label, passphrase=passphrase)
        try: 
            status = self.current_vault["keys"].checkKeyExpiration(key)
        except Exception as e:
            raise e
        return key

    def store(self, key, passphrase=None):
        """
        store the Key on the current vault by the label and a security passphrase
        param: passphrase: security passphrase for the key
        label, label of the key
        """
        if not isinstance(key, EncryptionKey):
            raise UnknownKeyType(key)
        try:
            self.current_vault["keys"].storeKey(key, passphrase=passphrase)
        except Exception as e:
            raise e
    
    def getKeyList(self, active=True):
        """
        List the Key in the current vault
        """
        if not isinstance(active, bool):
            raise UnknownType(active)
        try:
            return self.current_vault["keys"].listKeys(active)
        except Exception as e:
            raise e
           
    def checkKeyExpiration(self, key):
        """
        Check the expire on the related key
        param: key, the key that's going to be checked
        """
        if not isinstance(key, EncryptionKey):
            raise UnknownKeyType(key)
        try:
            return self.current_vault["keys"].checkKeyExpiration(key)
        except Exception as e:
            raise e
    
    def getKeyExpirationDate(self, key):
        """
        Get Date expiration on the related key
        """
        if not isinstance(key, EncryptionKey):
            raise UnknownKeyType(key)
        try:
            return self.current_vault["keys"].getKeyExpirationDate(key)
        except Exception as e:
            raise e

    def getCurrentKeysVault(self):
        """
        get the current vault to use with the keychain
        """
        return self.current_vault["keys"]
