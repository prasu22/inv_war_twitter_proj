import os
from cryptography.fernet import Fernet

def read_key():
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    file = open(f"{ROOT_PATH}/key.key", 'rb')
    key = file.read()
    file.close()
    return key

def encrypt_string(message):
    key  = read_key()
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(message.encode())
    return encrypted_data

def decrypt_string(message):
    key = read_key()
    fernet = Fernet(key)
    decrypt_data = fernet.decrypt(message)
    return decrypt_data.decode()


