import logging
import time

import boto3

import utils
from cse import KMSCryptoContext, S3CSE
from cse_performance_counters import CsePerformanceCounters

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


class S3CseClient:

    def __init__(self, key_id, perf_counters=None):
        operations_log = []
        self._s3_client = boto3.client("s3")
        self.key_id = key_id
        self._ctx = KMSCryptoContext(keyid=key_id, kms_client_args={'region_name': 'eu-west-2'})
        self._s3cse = S3CSE(crypto_context=self._ctx, s3_client=self._s3_client)
        self.last_operation_duration = 0
        self.perf_counters = perf_counters

    def write(self, bucket, filename, data):
        cse_used = self.key_id is not None
        if cse_used:
            encryption_msg = f"CSE using CMK {self.key_id}"
        else:
            encryption_msg = f"no CSE"

        logging.info(f"Writing object and its metadata to S3 ({encryption_msg})")
        start = time.process_time()
        response = self._s3cse.put_object(data, bucket, filename)
        finish = time.process_time()
        self.last_operation_duration = finish - start
        self.add_perf_counter(bucket,
                              filename,
                              CsePerformanceCounters.write,
                              cse_used,
                              self.last_operation_duration)
        logging.info(f"{filename} was writen in {utils.format_time_elapsed(self.last_operation_duration)}")
        return response

    def read(self, bucket, filename):
        start = time.process_time()
        logging.info("Downloading object and its metadata from S3")
        start = time.process_time()
        response = self._s3cse.get_object(bucket, filename)
        finish = time.process_time()
        self.last_operation_duration = finish - start
        self.add_perf_counter(bucket,
                              filename,
                              CsePerformanceCounters.read,
                              self.is_encrypted(response['Metadata']),
                              self.last_operation_duration)
        result = response['Body'].read()
        logging.info(f"{filename} was read in {utils.format_time_elapsed(self.last_operation_duration)}")
        return result

    def is_encrypted(self, metadata):
        return utils.is_encrypted(metadata)

    def get_metadata(self, bucket, filename, extended=False):
        logging.info("Retrieving object metadata from S3 without downloading the object itself")
        start = time.process_time()
        response = self._s3_client.head_object(Bucket=bucket, Key=filename)
        finish = time.process_time()
        self.last_operation_duration = finish - start
        self.add_perf_counter(bucket, filename, CsePerformanceCounters.head, None, self.last_operation_duration)
        logging.info(f"Metadata for {filename} was read in {utils.format_time_elapsed(self.last_operation_duration)}")
        if extended:
            result = response
        else:
            result = response['Metadata']
        return result

    def add_perf_counter(self, bucket, filename, operation, cse, duration):
        if self.perf_counters:
            self.perf_counters.add_counter(bucket, filename, operation, cse, duration)


if __name__ == '__main__':
    key_arn = 'arn:aws:kms:eu-west-2:299691842772:alias/SSE'
    bucket_name = 'leansec-sse-test-bucket'
    object_name = 'test.enc'

    s3_cse_client = S3CseClient(key_arn)
    s3_cse_client.key_id = key_arn
    resp = s3_cse_client.write(bucket_name, object_name, b'hello encrypted data')
    print(resp)
    data = s3_cse_client.read(bucket_name, object_name)
    print(data)
