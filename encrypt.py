from Crypto.Cipher import AES
import base64

def decryption(encrpted_string):
    secret_key = '@RCvws6`SMfF^).x'
    cipher = AES.new(secret_key,AES.MODE_ECB)
    decoded = cipher.decrypt(base64.b64decode(encrpted_string))
    decoded = str(decoded.strip())
    decoded = decoded.replace("b'","")
    decoded = decoded.replace("'","")
    return decoded