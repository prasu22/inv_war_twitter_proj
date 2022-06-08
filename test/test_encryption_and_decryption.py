import unittest
<<<<<<< HEAD
=======

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string, decrypt_string


class TestEncryptionDecryptionMethod(unittest.TestCase):

    def test_encryption_with_string(self):
        message = "this is new data 23424@#$@%😘"
        cipher_output = encrypt_string(message)
        output = decrypt_string(cipher_output)
<<<<<<< HEAD
        self.assertEqual(output, message)
=======
        self.assertEqual(output,message)
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0

    def test_encryption_without_string(self):
        message = ""
        cipher_output = encrypt_string(message)
        output = decrypt_string(cipher_output)
<<<<<<< HEAD
        self.assertEqual(message, output)


#

# if __name__ == '__main__':
#     unittest.main()
=======
        self.assertEqual(message,output)
#

if __name__ == '__main__':
    unittest.main()
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
