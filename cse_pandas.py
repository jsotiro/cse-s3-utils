import json
import time

import pandas as pd
import io
from s3_cse_client import S3CseClient


def read_encrypted_parquest_df(key_id, bucket, filename):
    s3 = S3CseClient(key_id)
    data = s3.read(bucket, filename)
    f_in = io.BytesIO()
    f_in.write(data)
    # create a new data frame from the  in mem buffer
    new_df = pd.read_parquet(f_in)
    return new_df


def write_encrypted_parquest_df(key_id, df, filename, engine='pyarrow', compression='snappy', index=False):
    s3 = S3CseClient(key_id)
    f_out = io.BytesIO()
    df.to_parquet(path=f_out, engine=engine, compression=compression, index=index)
    f_out.seek(0)
    parquet_data = f_out.read()
    object_key = filename()
    s3.write(bucket_name, object_key, parquet_data)
    return object_key


def simple_filename():
    object_name_prefix = 'data'
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "{}-{}.parquet".format(object_name_prefix, timestr)
    return filename


if __name__ == '__main__':
    key_arn = 'arn:aws:kms:eu-west-2:299691842772:alias/SSE'
    bucket_name = 'leansec-sse-test-bucket'
    sample_dict = {}
    json_file = open('./sample.json')
    try:
        sample_dict = json.load(json_file)
    finally:
        json_file.close()
    df = pd.DataFrame(sample_dict['students'])
    name = write_encrypted_parquest_df(key_arn, df, simple_filename)
    print(name)
    new_df = read_encrypted_parquest_df(key_arn, bucket_name, name)
    print(new_df)
    new_df.to_csv('test.csv')