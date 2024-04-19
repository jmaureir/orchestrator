# Shamir’s Secret Implementation module for Credential Manager

import json
import ast
import base64
from binascii import hexlify,unhexlify
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Protocol.SecretSharing import Shamir
from .Token import Token
from .loggers import BasicLogger
from .exceptions import *

class ShamirImplementation(BasicLogger):
    """
    Shamir’s shared secret Implementation
    """

    def __init__(self):
        BasicLogger.__init__(self,self.__class__.__name__)
    
    def generate_token(self):
        new_key = get_random_bytes(16)
        return new_key

    def encryptSecret(self, credential, n_unlock, shared_users):
        """
        Encrypt a crendential (the shared secret) with n unlock tokens from shared_users total tokens
        """
        if credential.type_ == 2 :
            raise CredentialAlreadyDigested(credential.label)

        key = self.generate_token()
        shared_keys = []
        user_token = []
        shares = Shamir.split(n_unlock, shared_users, key)
        
        for idx, share in shares:
            if idx == 1:
                token_bytes = str((idx, hexlify(share))).encode("ascii")
                b64_token = base64.b64encode(token_bytes).decode("ascii")
                
                user_token.append(b64_token)
            else:
                token_bytes = str((idx, hexlify(share))).encode("ascii")
                b64_token = base64.b64encode(token_bytes).decode("ascii")
                token_byte = b64_token.encode("ascii")
                shared_keys.append(Token(label=credential.getLabel(), token=token_byte, type_=3, whom=None, comment=None, creation=None, expiration=None))
        
        cipher = AES.new(key, AES.MODE_EAX)
        credential_content =  str(credential.content).encode("ascii")
        cipher = AES.new(key, AES.MODE_EAX)
        nonce = cipher.nonce
        cipher_credential, tag = cipher.encrypt_and_digest(credential_content)
        
        credential.updateTag(tag)
        credential.updateNonce(nonce)
        credential.updateToken(user_token[0])
        credential.updateType(2)
        credential.updateContent(cipher_credential)

        return shared_keys

    def decryptSecret(self, tokens, credential):
        """
        decrypt a credential with given tokens
        """
       # Key recontruction process
        retrieve_tokens= []
        for token_object in tokens:
            token = token_object.getToken()
            retrieve_tokens.append(token)
        
        # tokens are still serialized
        retrieve_tokens.append(credential.token)
        
        shares = []        
        for token in retrieve_tokens:
            # this is awful, but we keep it for backwards compat           
            token_bytes = base64.b64decode(token).decode("ascii")
            token_raw = ast.literal_eval(token_bytes)

            stripped_token = str(token_raw).replace(')','').replace('(','').replace(" b'","").replace("'","")
            idx, share = [ s.strip() for s in stripped_token.split(",") ]
            shares.append((int(idx), unhexlify(share)))
                    
        key = Shamir.combine(shares)
        
        # We recover and decrypt the message
        nonce = credential.nonce
        tag   = credential.tag
        
        cipher_credential = base64.b64decode(credential.content)
        
        cipher = AES.new(key, AES.MODE_EAX,nonce=nonce)
        try:
            credential_decrypt = cipher.decrypt(cipher_credential)
            cipher.verify(tag)
            decoded_content = credential_decrypt.decode('ascii')
            dict_content = ast.literal_eval(decoded_content)
            credential.updateContent(dict_content)
            credential.updateNonce(None)
            credential.updateToken(None)
            credential.updateTag(None)
            credential.updateType(1)
            
        except:
            # try to decode double b64 encoded payload
            try:
                b64_cipher_crd   = base64.b64decode(credential.content).decode("ascii").replace('\"',"")
                cipher_credential = base64.b64decode(b64_cipher_crd)
                cipher = AES.new(key, AES.MODE_EAX,nonce=nonce)
                credential_decrypt = None
                try:
                    credential_decrypt = cipher.decrypt(cipher_credential)
                except Exception as e:
                    raise e
                cipher.verify(tag)
                decoded_content = credential_decrypt.decode('ascii')
                dict_content = ast.literal_eval(decoded_content)
                credential.updateContent(dict_content)
                credential.updateNonce(None)
                credential.updateToken(None)
                credential.updateTag(None)
                credential.updateType(1)
            except:
                #credential.updateContent(credential_decrypt)
                credential.updateNonce(None)
                credential.updateToken(None)
                credential.updateTag(None)
                credential.updateType(4)
             
