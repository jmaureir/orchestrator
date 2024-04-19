# Exception module for Credential Manager

class MultipleCredentialsFound(Exception):
    def __init__(self, label):
        super(MultipleCredentialsFound, self).__init__("Label: %s" % (label))


class KeyIsNotInstance(Exception):
    def __init__(self):
        super(KeyIsNotInstance, self).init("The Key is not an Instance of Encryption")

class NoExpirationDateFound(Exception):
    def __init__(self):
        super(NoExpirationDateFound, self).__init__("No Expiration date on keys found")


class ProtectedEncryptionKey(Exception):
    def __init__(self):
        super(ProtectedEncryptionKey, self).__init__("Protected Encryption Key")

        
class LabelNecessary(Exception):
    def __init__(self):
        super(LabelNecessary, self).__init__("Label is necessary")
        
        
class PassphraseNecessary(Exception):
    def __init__(self):
        super(PassphraseNecessary, self).__init__("Passphrase is necessary for private key")

        
class PassphraseNotNecessary(Exception):
    def __init__(self):
        super(PassphraseNotNecessary, self).__init__("Passphrase is not necessary for public key")

        
class ExpiredKey(Exception):
    def __init__(self):
        super(ExpiredKey, self).__init__("Key expired")
        
        
class ExpiredCredential(Exception):
    def __init__(self):
        super(ExpiredCredential, self).__init__("Credential expired")
        
        
class ExpiredToken(Exception):
    def __init__(self):
        super(ExpiredToken, self).__init__("Token expired")
        

class PastExpirationDate(Exception):
    def __init__(self):
        super(PastExpirationDate, self).__init__("Expiration date is before today")


class WrongKeyPassphrase(Exception):
    def __init__(self):
        super(WrongKeyPassphrase, self).__init__("The passphrase for the key is wrong")

        
class WrongCredential(Exception):
    def __init__(self):
        super(WrongCredential, self).__init__("The credential is wrong")
        

class CredentialNotFound(Exception):
    def __init__(self, label):
        super(CredentialNotFound, self).__init__("Credential with Label: %s, not found on database" % (label))

        
class KeyNotFound(Exception):
    def __init__(self, label):
        super(KeyNotFound, self).__init__("Key with label: %s, not found on database" %(label))
        

class TokenNotFound(Exception):
    def __init__(self, label):
        super(TokenNotFound, self).__init__("Token for Label: %s, not found on database" % (label))
        
        
class TokenNotAvailable(Exception):
    def __init__(self, label):
        super(TokenNotAvailable, self).__init__("Token for Label: %s, not availabe" % (label))
             

class CredentialAlreadyExists(Exception):
    def __init__(self, label):
        super(CredentialAlreadyExists, self).__init__("Credential with Label: %s, already exists" % (label))

        
class KeyAlreadyExists(Exception):
    def __init__(self, label):
        super(KeyAlreadyExists, self).__init__("Key with label: %s, already exists" %(label))

        
class TokenAlreadyExists(Exception):
    def __init__(self, label):
        super(TokenAlreadyExists, self).__init__("Token for Label: %s, already exists" % (label))
        
        
class TokenAlreadyAssigned(Exception):
    def __init__(self, label):
        super(TokenAlreadyAssigned, self).__init__("Token for Label: %s, already assigned for user" % (label))
        
        
class UnknownCredentialType(Exception):
    def __init__(self, arg):
        super(UnknownCredentialType, self).__init__(type(arg))

        
class UnknownType(Exception):
    def __init__(self, arg):
        super(UnknownType, self).__init__(type(arg))

        
class UnknownKeyType(Exception):
    def __init__(self, arg):
        super(UnknownKeyType, self).__init__(type(arg))
   
    
class UnknownTokenType(Exception):
    def __init__(self, arg):
        super(UnknownTokenType, self).__init__(type(arg))


class EmptyCredential(Exception):
    def __init__(self, label):
        super(EmptyCredential, self).__init__(label)

                
class LocalKeyNotFound(Exception):
    def __init__(self, label):
        super(LocalKeyNotFound, self).__init__("Local key with label: %s, not found" %(label))
        

class LocalCredentialNotFound(Exception):
    def __init__(self, label):
        super(LocalCredentialNotFound, self).__init__("Local credential with label: %s, not found" %(label))


class MultipleKeysFound(Exception):
    def __init__(self, label):
        super(MultipleKeysFound, self).__init__(label)
        
        
class AlreadyExists(Exception):
    def __init__(self, type_, label):
        super().__init__("%s with label: %s, already exists" %(type_, label))
        

class AlreadyStored(Exception):
    def __init__(self,type_, label):
        super(AlreadyStored, self).__init__("%s with label: %s, already stored" %(type_, label))
        
        
class AlreadyEncrypted(Exception):
    def __init__(self,type_, label):
        super(AlreadyEncrypted, self).__init__("%s with label: %s, already encrypted, it must be flat" %(type_, label))
        
        
class KeyNotLoaded(Exception):
    def __init__(self):
        super(KeyNotLoaded, self).__init__()
        
        
class PrivateKeyNotLoaded(Exception):
    def __init__(self):
        super(PrivateKeyNotLoaded, self).__init__("there is no private key to perform action")


class StoreCredentialError(Exception):
    def __init__(self):
        super(StoreCredentialError, self).__init__("could not store credential")


class StoreCredentialFileError(Exception):
    def __init__(self):
        super(StoreCredentialFileError, self).__init__("could not store credential in file")
        
        
class CredentialNotDecrypted(Exception):
    def __init__(self):
        super(CredentialNotDecrypted, self).__init__("credential could not be decrypted")
        
        
class KeyNotFound(Exception):
    def __init__(self, label):
        super(KeyNotFound, self).__init__("Key with label: %s, not found on database" %(label))
        
class hasChanged(Exception):
    def __init__(self, label):
        super(hasChanged, self).__init__("%s has changed" %(label))

class VaultNotFound(Exception):
    def __init__(self, vault_type):
        super(VaultNotFound, self).__init__(vault_type)
        
class CredentialAlreadyDigested(Exception):
    def __init__(self, label):
        super(CredentialAlreadyDigested, self).__init__(label)
