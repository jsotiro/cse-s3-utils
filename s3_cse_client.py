import base64
import logging
import struct
import boto3
import io
import os
from Crypto.Cipher import AES

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


class S3CseClient:

    def __init__(self, key_id=None):
        self.kms = boto3.client("kms")
        self.chunksize = 64 * 1024
        self.s3 = boto3.client('s3')
        self.key_id = key_id

    def write(self, bucket, filename, data):
        # Generate a Data Key (encrypted with KMS CMK)
        key = self.kms.generate_data_key(KeyId=self.key_id, KeySpec='AES_256')
        dataKeyPlain = key['Plaintext']
        dataKey = key['CiphertextBlob']
        # Encrypt bytes with the data key
        logging.info("Creating the IV and cipher")
        iv = os.urandom(16)
        cipher = AES.new(dataKeyPlain, AES.MODE_GCM, iv)

        logging.info("Plain-text data key = %s " % base64.b64encode(dataKeyPlain))
        logging.info("Encrypted data key  = %s " % base64.b64encode(dataKey))
        logging.info("Encrypting data")
        data_size = len(data)
        f_in = io.BytesIO()
        f_in.write(data)
        f_in.seek(0)

        f_out = io.BytesIO()
        f_out.write(struct.pack('<Q', data_size))
        f_out.write(iv)
        chunk = f_in.read(self.chunksize)
        while len(chunk) != 0:
            if len(chunk) % 16 != 0:
                chunk += b' ' * (16 - len(chunk) % 16)
                enc_buffer = cipher.encrypt(chunk)
                f_out.write(enc_buffer)
                chunk = f_in.read(self.chunksize)
        #
        # Store encrypted file on S3
        # Encrypted Key will be stored as meta data
        #
        logging.info("Storing encrypted file on S3")
        metadata = {
            "key": base64.b64encode(dataKey).decode("utf-8")
        }
        f_out.seek(0)
        encrypted_data = f_out.getvalue()
        response = self.s3.put_object(Bucket=bucket,
                                      Key=filename,
                                      ContentType='binary/data',
                                      Body=encrypted_data, Metadata=metadata)
        return response

    def read(self, bucket, filename):
        # download encrypted object and it's metadata
        logging.info("Download object and its metadata from S3")
        response = self.s3.get_object(Bucket=bucket,
                                      Key=filename)
        #
        # retrieve data key from response.metadata
        dataKey = base64.b64decode(response['Metadata']['key'])
        # decrypt encrypted key
        logging.info("Decrypt data key")
        key = self.kms.decrypt(KeyId=self.key_id, CiphertextBlob=dataKey)
        dataKeyPlain = key['Plaintext']
        logging.info("Plain text data key = %s " % base64.b64encode(dataKeyPlain))
        logging.info("Encrypted  data key  = %s " % base64.b64encode(dataKey))

        logging.info("Decrypt the object")
        enc_data = response['Body'].read()
        f_in = io.BytesIO()
        f_in.write(enc_data)
        f_in.seek(0)
        origsize = struct.unpack('<Q', f_in.read(struct.calcsize('Q')))[0]
        iv = f_in.read(16)
        cipher = AES.new(dataKeyPlain, AES.MODE_GCM, iv)
        chunk = f_in.read(self.chunksize)
        f_out = io.BytesIO()
        while len(chunk) != 0:
            f_out.write(cipher.decrypt(chunk))
            chunk = f_in.read(self.chunksize)
            f_out.truncate(origsize)
        f_out.seek(0)
        data = f_out.getvalue()
        return data

if __name__ == '__main__':
    key_arn = 'arn:aws:kms:eu-west-2:299691842772:alias/SSE'
    bucket_name = 'leansec-sse-test-bucket'
    object_name = 'test.enc'

    s3_cse_client = S3CseClient()
    s3_cse_client.key_id = key_arn
    resp = s3_cse_client.write(bucket_name, object_name, b'hello encrypted data')
    print(resp)
    data = s3_cse_client.read(bucket_name, object_name)
    print(data)
