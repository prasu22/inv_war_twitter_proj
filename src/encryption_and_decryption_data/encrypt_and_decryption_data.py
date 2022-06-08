import os
from cryptography.fernet import Fernet

def read_key():
    """
        read the root path and open the key to get the encryption key
        :param
        file : store the file object
        key: store the encryption key
        :return
        return the key
    """
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    file = open(f"{ROOT_PATH}/key.key", 'rb')
    key = file.read()
    file.close()
    return key

def encrypt_string(message):
    """
        encrypt the incomming messages string using fernet
        :param
         key : store the encryption key
         fernet: store the fernet object
         encrypted_data: store the encrypted string
         :return
         return the encrypted string
    """
    key  = read_key()
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(message.encode())
    return encrypted_data

def decrypt_string(message):
    """
        decrypt the incomming encrypted message string
        :param
        key: store the encrypted key
        fernet: store the fernet object
        decrypt_data: store the decrypted data
        :return
        return the decrypted string
    """
    key = read_key()
    fernet = Fernet(key)
    decrypt_data = fernet.decrypt(message)
    return decrypt_data.decode()


