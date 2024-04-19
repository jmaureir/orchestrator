# RSA Implementation module for Credential Manager

import base64
import json
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15

class RSAImplementation:
    """
            RSA Implementation
            Here we generate the keys using RSA methods
    """

    max_msg_length=212
    
    def __init__(self):
        pass

    def generate(self):
        new_key = RSA.generate(2048)
        return new_key

    def encrypt(self, msg, key):
        """
        This method encrypt data based on a public key
        :param public_key:
        :param data:
        :return: the data encrypted
        """
        encryptor = PKCS1_OAEP.new(key.getPublicKey())
        
        if len(msg)>self.max_msg_length:
            # divide the message in chunks of 212 bytes
            chunks = [msg[i:i+self.max_msg_length] for i in range(0, len(msg), self.max_msg_length)]
            lst_encrypted = []
            for chunk in chunks:
                lst_encrypted.append(base64.b64encode(encryptor.encrypt(chunk)).decode("utf8"))
            
            encrypted = base64.b64encode(json.dumps(lst_encrypted).encode("utf8"))
        else:
            encrypted = base64.b64encode(json.dumps([ base64.b64encode(encryptor.encrypt(msg)).decode("utf8") ]).encode("utf8"))

        return encrypted

    def decrypt(self, encrypted, key):
        """
        This method decrypts the data based on a private key
        :param private_key: The private key required for decryption
        :param encrypted: The encrypted data that's looking to be decrypted
        :return: the data decrypted
        """

        json_str = base64.b64decode(encrypted)
        lst_encrypted = json.loads(json_str.decode("utf8"))
                
        decryptor = PKCS1_OAEP.new(key.getPrivateKey())        
        lst_msg = []
        for encrypted_chunk in lst_encrypted:
            lst_msg.append(decryptor.decrypt(base64.b64decode(encrypted_chunk)).decode("utf8") )
        
        msg = "".join(lst_msg)
        return msg
    
    def signature(self, key, msg, owner):
        private_key = key.getPrivateKey
        hash_msg = SHA256.new(msg)

        owner = pkcs1_15.new(private_key)
        signature = owner.sign(hash_msg)

        return signature

    def verifySignature(self, key, msg, original_msg):
        public_key = key.getPublicKey
        try:
            pkcs1_15.new(public_key).verify(msg, original_msg)
        except (ValueError, TypeError):
            raise hasChanged("Data")

