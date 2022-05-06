import unittest

from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string, decrypt_string


class TestEncryptionDecryptionMethod(unittest.TestCase):

    def test_encryption_with_string(self):
        message = "this is new data 23424@#$@%😘"
        cipher_output = encrypt_string(message)
        output = decrypt_string(cipher_output)
        self.assertEqual(output,message)

    def test_encryption_without_string(self):
        message = ""
        cipher_output = encrypt_string(message)
        output = decrypt_string(cipher_output)
        self.assertEqual(message,output)
#

if __name__ == '__main__':
    unittest.main()
