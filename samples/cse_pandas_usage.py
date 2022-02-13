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
    name = cse_pd.write_parquet_df( df,bucket_name, simple_filename, cmk_id=key_arn,)
    print(name)
    new_df = cse_pd.read_parquet_df(bucket_name, name)
    print(new_df)
    new_df.to_csv('test.csv')


if __name__ == '__main__':
    sample_json_test()

