import time
import json

import pandas as pd

import cse_pandas as cse_pd

def simple_filename():
    object_name_prefix = 'data'
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "{}-{}.parquet".format(object_name_prefix, timestr)
    return filename


def sample_json_test():
    key_arn = 'arn:aws:kms:eu-west-2:299691842772:alias/SSE'
    bucket_name = 'leansec-sse-test-bucket'
    sample_dict = {}
    json_file = open('./sample.json')
    try:
        sample_dict = json.load(json_file)
    finally:
        json_file.close()
    df = pd.DataFrame(sample_dict['students'])
    name = cse_pd.write_encrypted_parquest_df(key_arn, df,bucket_name, simple_filename)
    print(name)
    #key_arn = 'arn:aws:kms:eu-west-2:325310171117:alias/rtg-dev-crep-cse-current'
    #bucket_name = 'dev-cre-sec-published-bucket-01'
    #name = "data-20220208-194051.parquet"
    new_df = cse_pd.read_encrypted_parquest_df(key_arn, bucket_name, name)
    print(new_df)
    new_df.to_csv('test.csv')


def read_an_s3_parquet():
    #  filename = "crep_parquet/jf3-advances/consolidated-data/ClaimantPersonalDetail/year=2022/month=02/day=02/17c46121-141b-4bb8-9d95-2de937104f1a.parquet"
    filename = "crep_parquet/test.parquet"
    key_arn = 'arn:aws:kms:eu-west-2:325310171117:alias/rtg-dev-cre-cse-current'
    bucket_name = 'dev-cre-published-zone-04'
    new_df = cse_pd.read_encrypted_parquest_df(key_arn, bucket_name, filename)
    print(new_df)
    new_df.to_csv('test2.csv')


if __name__ == '__main__':
    sample_json_test()
