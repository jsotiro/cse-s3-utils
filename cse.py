import boto3
import base64
import json
import os

from io import BytesIO
from typing import Dict, Optional, Any, Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.exceptions import InvalidTag

AES_BLOCK_SIZE = 128
AES_BLOCK_SIZE_BYTES = 16


# Just so it looks like the object s3 GetObject returns
class DummyAIOFile(object):
    """So that response['Body'].read() presents the same way as a normal S3 get"""

    def __init__(self, data: bytes):
        self.file = BytesIO(data)

    def read(self, n=-1):
        return self.file.read(n)

    def readany(self):
        return self.file.read()

    def readexactly(self, n):
        return self.file.read(n)

    def readchunk(self):
        return self.file.read(), True


class DecryptError(Exception):
    pass


class CryptoContext(object):
    def setup(self):
        pass

    def enabled(self):
        return True

    def get_decryption_aes_key(self, key: bytes, material_description: Dict[str, Any]) -> bytes:
        """
        Get decryption key for a given S3 object
        :param key: Base64 decoded version of x-amz-key-v2
        :param material_description: JSON decoded x-amz-matdesc
        :return: Raw AES key bytes
        """
        raise NotImplementedError()

    def get_encryption_aes_key(self) -> Tuple[bytes, Dict[str, str], str]:
        """
        Get encryption key to encrypt an S3 object
        :return: Raw AES key bytes, Stringified JSON x-amz-matdesc, Base64 encoded x-amz-key-v2
        """
        raise NotImplementedError()


class KMSCryptoContext(CryptoContext):
    """
    Crypto context which uses symmetric cryptography.
    The key field should be a valid AES key.
    E.g. if you wanted to set the KMS region, add kms_client_args={'region_name': 'eu-west-1'}
    :param keyid: Key bytes
    :param kms_client_args: Will be expanded when getting a KMS client
    :param authenticated_encryption: Uses AES-GCM instead of AES-CBC (also allows range gets of files)
    """

    def __init__(self, keyid: Optional[str] = None, kms_client_args: Optional[dict] = None,
                 authenticated_encryption: bool = True):
        self.kms_key = keyid
        self.authenticated_encryption = authenticated_encryption

        # Store the client instead of creating one every time, performance wins when doing many files
        self._kms_client = boto3.client("kms")
        self._kms_client_args = kms_client_args if kms_client_args else {}

    def enabled(self):
        return self.kms_key is not None

    def get_decryption_aes_key(self, data_key: bytes, material_description: Dict[str, Any]) -> bytes:
        if self.kms_key is None:
            self.kms_key = material_description['kms_cmk_id']
        if self.kms_key is None:
            raise ValueError('KMS Key not provided during initialisation, cannot decrypt data key')
        kms_response = self._kms_client.decrypt(KeyId=self.kms_key, CiphertextBlob=data_key)
        return kms_response['Plaintext']

    def get_kms_arn_id(self ):
        response = self._kms_client.describe_key(KeyId=self.kms_key)
        return response['KeyMetadata']['Arn']

    def get_encryption_aes_key(self) -> Tuple[bytes, Dict[str, str], str]:
        if self.kms_key is None:
            raise ValueError('KMS Key not provided during initialisation, cannot generate data key')
        self.kms_key = self.get_kms_arn_id()
        encryption_context = {'kms_cmk_id': self.kms_key}
        key_response = self._kms_client.generate_data_key(KeyId=self.kms_key, KeySpec='AES_256')
        return key_response['Plaintext'], encryption_context, base64.b64encode(key_response['CiphertextBlob']).decode()


class S3CSE(object):
    """
    S3 Client-side encryption wrapper.
    To change S3 region add s3_client_args={'region_name': 'eu-west-1'}
    To use this object, 
    :param crypto_context: Takes a crypto context 
    :param s3_client_args: Optional dict of S3 client args
    """

    def __init__(self, crypto_context: CryptoContext, s3_client=None, s3_client_args: Optional[dict] = None):
        self._backend = default_backend()
        self._crypto_context = crypto_context
        self._session = None
        self._s3_client = s3_client
        self._s3_client_args = s3_client_args if s3_client_args else {}

    def boto3_s3(self):
        return  self._s3_client

    def setup(self):
        self._s3_client = boto3.client("s3")

    # noinspection PyPep8Naming
    def get_object(self, Bucket: str, Key: str, **kwargs) -> dict:
        """
        S3 GetObject. Takes same args as Boto3 documentation
        Decrypts any CSE
        :param Bucket: S3 Bucket
        :param Key: S3 Key (filepath)
        :return: returns same response as a normal S3 get_object
        """
        if self._s3_client is None:
            self.setup()

        s3_response = self._s3_client.get_object(Bucket=Bucket, Key=Key)
        metadata = s3_response['Metadata']
        whole_file_length = int(s3_response['ResponseMetadata']['HTTPHeaders']['content-length'])
        if 'x-amz-key' not in metadata and 'x-amz-key-v2' not in metadata:
            return s3_response

        if 'x-amz-key' in metadata:
            # Crypto V1
            # Todo move the file obj into the decrypt to do streaming
            file_data = s3_response['Body'].read()
            body = self._decrypt_v1(file_data, metadata)
        else:
            # Crypto V2
            # Todo move the file obj into the decrypt to do streaming
            file_data = s3_response['Body'].read()
            body = self._decrypt_v2(file_data, metadata, whole_file_length)
        s3_response['Body'] = DummyAIOFile(body)
        return s3_response

    def _decrypt_v1(self, file_data: bytes, metadata: Dict[str, str], range_start: Optional[int] = None) -> bytes:
        if range_start:
            raise DecryptError('Cant do range get when not using KMS encryption')

        decryption_key = base64.b64decode(metadata['x-amz-key'])
        material_description = json.loads(metadata['x-amz-matdesc'])

        aes_key = self._crypto_context.get_decryption_aes_key(decryption_key, material_description)

        # x-amz-key - Contains base64 encrypted key
        # x-amz-iv - AES IVs
        # x-amz-matdesc - JSON Description of client-side master key (used as encryption context as is)
        # x-amz-unencrypted-content-length - Unencrypted content length

        iv = base64.b64decode(metadata['x-amz-iv'])

        # TODO look at doing AES as stream

        # AES/CBC/PKCS5Padding
        aescbc = Cipher(AES(aes_key), CBC(iv), backend=self._backend).decryptor()
        padded_result = aescbc.update(file_data) + aescbc.finalize()

        unpadder = PKCS7(AES.block_size).unpadder()
        result = unpadder.update(padded_result) + unpadder.finalize()
        return result

    def _decrypt_v2(self, file_data: bytes, metadata: Dict[str, str], entire_file_length: int,
                    range_start: Optional[int] = None, desired_start: Optional[int] = None,
                    desired_end: Optional[int] = None) -> bytes:

        decryption_key = base64.b64decode(metadata['x-amz-key-v2'])
        material_description = json.loads(metadata['x-amz-matdesc'])
        aes_key = self._crypto_context.get_decryption_aes_key(decryption_key, material_description)

        # x-amz-key-v2 - Contains base64 encrypted key
        # x-amz-iv - AES IVs
        # x-amz-matdesc - JSON Description of client-side master key (used as encryption context as is)
        # x-amz-unencrypted-content-length - Unencrypted content length
        # x-amz-wrap-alg - Key wrapping algo, either AESWrap, RSA/ECB/OAEPWithSHA-256AndMGF1Padding or KMS
        # x-amz-cek-alg - AES/GCM/NoPadding or AES/CBC/PKCS5Padding
        # x-amz-tag-len - AEAD Tag length in bits




        iv = base64.b64decode(metadata['x-amz-iv'])
        # TODO look at doing AES as stream
        if metadata.get('x-amz-cek-alg', 'AES/CBC/PKCS5Padding') == 'AES/GCM/NoPadding':
            aesgcm = AESGCM(aes_key)
            try:
                result = aesgcm.decrypt(iv, file_data, None)
            except InvalidTag:
                raise DecryptError('Failed to decrypt, AEAD tag is incorrect. Possible key or IV are incorrect')
        else:
            # AES/CBC/PKCS5Padding
            aescbc = Cipher(AES(aes_key), CBC(iv), backend=self._backend).decryptor()
            padded_result = aescbc.update(file_data) + aescbc.finalize()
            unpadder = PKCS7(AES.block_size).unpadder()
            result = unpadder.update(padded_result) + unpadder.finalize()
        return result

    def put_object(self, Body: bytes, Bucket: str, Key: str, Metadata: Dict = None, **kwargs):
        """
        PutObject. Takes same args as Boto3 documentation
        Encrypts files
        :param: Body: File data
        :param Bucket: S3 Bucket
        :param Key: S3 Key (filepath)
        :param Metadata: S3 Key (filepath)
        """
        if self._s3_client is None:
            self.setup()
        Metadata = Metadata if Metadata is not None else {}
        if self._crypto_context.enabled():
            # noinspection PyUnresolvedReferences
            authenticated_crypto = self._crypto_context.authenticated_encryption
            aes_key, matdesc_metadata, key_metadata = self._crypto_context.get_encryption_aes_key()

            if authenticated_crypto:
                Metadata['x-amz-cek-alg'] = 'AES/GCM/NoPadding'
                Metadata['x-amz-tag-len'] = str(AES_BLOCK_SIZE)
                iv = os.urandom(12)

                # 16byte 128bit authentication tag forced
                aesgcm = AESGCM(aes_key)
                result = aesgcm.encrypt(iv, Body, None)

            else:
                # V1 is always AES/CBC/PKCS5Padding
                Metadata['x-amz-cek-alg'] = 'AES/CBC/PKCS5Padding'
                iv = os.urandom(16)
                padder = PKCS7(AES.block_size).padder()
                padded_result = padder.update(Body) + padder.finalize()
                aescbc = Cipher(AES(aes_key), CBC(iv), backend=self._backend).encryptor()
                result = aescbc.update(padded_result) + aescbc.finalize()

            # For all V1 and V2
            Metadata['x-amz-unencrypted-content-length'] = str(len(Body))
            Metadata['x-amz-iv'] = base64.b64encode(iv).decode()
            Metadata['x-amz-matdesc'] = json.dumps(matdesc_metadata)

            Metadata['x-amz-wrap-alg'] = 'kms'
            Metadata['x-amz-key-v2'] = key_metadata
            Body = result

        response = self._s3_client.put_object(
            Bucket=Bucket,
            Key=Key,
            Body=Body,
            Metadata=Metadata,
            **kwargs
        )

        return response
