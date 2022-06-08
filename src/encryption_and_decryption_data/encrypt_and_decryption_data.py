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

# message = "Z0FBQUFBQmllS1lMdHh3SHVmbkpzR3c5U0tobGNiZC1Sam0yWjFnc2NyZ25NOFNwTDVqRmdua0k4RnR3dkU4SWtNS3VhV1hhMS11R0FvRU1jX1gzdjZERGdSYVdxcktESjlHZ0tuUXBhSXVFX2F4LXJNRlE3Q3RYQ2ZjWGxEc2FXTEFrMGozUzVxVU9rT19WRXpKX0NDZktQVHNpaktVUm41N2tMcEhoa3RSaDc4bmdWcEtFWFRzN2NXdzFtQ3VuWGhXaU8zQW9ReXBTZmw0WUhGUDQ3Wkg5TEtjWlNqMlp1OE8yRWpMUU1MNllVM3FUeVBTckxEV2JFODBNTzQ3eW15WGJ4ZjVZUWRNd1gzM25iakwwSmUwSVJuMmZqNElrWW9uZ19mY1dhQzhUZGVUMkV5MFQzTzNRRVFhWFNvQnNsS3UwV0ZyaXFKRjJoVll0THhicldPRDVDcDRCbDRrV2RHS0w3elF1NUVteGk3amdmRU44blhMZFRVb0JnRzNXNUNLam00SXFBcTNLMG82VVYzby1PSHpOLWtPT3o1NGt6clNkRjZNMFpseUNfcVBzTFRpT0ZnOEpHNXhxNjJXTzBRUk9nR0F2dWdsaw=="
# data = "hello"
# message = encrypt_string(data)
# print(message)
# print(decrypt_string(message))