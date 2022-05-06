# Encryption in Python
There are two type of encryption in python:-
1. Symmetric Encryption
2. Asymmetric Encryption

### Symmtric Encryption
In symmetric Encryption We use single key for encryption and decryption.

### Asymmetric Encryption
In Asymmetric Encryption we use two public and private key for encryption and decryption. The public key is available on internet so the sender can encrypt the message using public key and the private is available only to the receiver so the receiver only can decrypt that message.

### Symmertic encryption used in this project

For encryption and decryption the data, first we need to install some dependensies
> $pip install cryptography

### import some libraries which we need to use
> import base64
> 
> from cryptography.hazmat.primitives import hashes
> 
> from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

- base64 is used to convert bytes that have binary or text data into ASCII Characters.

- Hashing is the process in which you convert a block of data of arbitrary size to a fixed-size value.

- PBKDF2 (Password Based Key Derivation Function 2) is typically used for deriving a cryptographic key from a password.

> 
> password_provided = <String>
> 
> password = password_provided.encode()
> 
> salt = os.urandom(16)
> 
> kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000)
> 
> key = base64.urlsafe_b64encode(kdf.derive(password))

In above code we assign a string to password_provided, which is converted in byte using encode function and assign to password. we create a random byte string and assign it to salt. Now the **PBKDF2HMAC** create an object to derive the key based on password. Using **base64.urlsafe_b64encode** we can encode the string using url and file system safe alphabets into the binary form.
now key is created. so we save this key in file to use it later.

>import os
> 
>from cryptography.fernet import Fernet

- OS module in Python provides functions for creating and removing a directory (folder), fetching its contents, changing and identifying the current directory, etc.

- Fernet guarantees that a message encrypted using it cannot be manipulated or read without the key

> def read_key():

- it is used to read the key which we created.
> def encryption_string(normal_message):

- it is used to encrypt the passed string message using the fernet library and the key

>def decryption_string(chipher_message):

- it is used to decrypt the encrypted data using fernet library and the key.
