import cse_pandas as cse_pd
import utils
from  s3_cse_client import  S3CseClient
import pandas as pd
import matplotlib.pyplot as plt


cse_cmk_us = "arn:aws:kms:us-east-1:299691842772:alias/cse-us"
bucket = "sagemaker-studio-t9sc0jup0m8"
filename = "bank-additional/bank-additional-full.csv"
cse_s3 = S3CseClient(cse_cmk_us)

df = cse_pd.read_csv_df( bucket, filename )
# write an unencrypted copy
print(df.shape)


metadata = cse_s3.get_metadata(bucket, filename)
print(metadata)

name = cse_pd.write_csv_df(df, bucket, "bank-additional/bank-additional-full-copy.csv",
                           index=False)
# read the unencrypted copy
df_copy = cse_pd.read_csv_df(bucket, name)
print(df_copy.shape)
print(df_copy.equals(df))


name = cse_pd.write_csv_df(df, bucket,
                           "bank-additional/bank-additional-full-copy-encrypted.csv",
                           cmk_id=cse_cmk_us, index=False)

metadata = cse_s3.get_metadata(bucket, name)
print(f"file is encrypted {utils.is_encrypted(metadata)}")
df = cse_pd.metadata_df(metadata)
print('S3 Metadata')
print(df)

df_copy_enc = cse_pd.read_csv_df(bucket, name, cmk_id=cse_cmk_us)
print(df_copy_enc.shape)
print(df_copy_enc.equals(df))
counters_df = cse_pd.get_performance_counters()
counters_df = counters_df.iloc[1:, :]
summary_df = utils.counters_summary_df(counters_df, 'operation', 'cse', 'duration')

summary_df.plot(kind="bar")
plt.show()