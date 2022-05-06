import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

password_provided = 'twitter_project'
password = password_provided.encode()

salt = b'8\xd1\x141^\xb0Y\xd3c4\x1dXbG\xfb\xe8'

kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000)

key = base64.urlsafe_b64encode(kdf.derive(password))
# print(key)
file = open('key.key', 'wb')
file.write(key)
file.close()
