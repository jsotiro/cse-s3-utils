import cse_pandas as cse_pd
import s3_cse_client as cse_s3

df = cse_pd.read_csv_df("sagemaker-studio-t9sc0jup0m8", "bank-additional/bank-additional-full.csv")
# write an unencrypted copy
print(df.shape)
name = cse_pd.write_parquet_df(df, "sagemaker-studio-t9sc0jup0m8", "bank-additional/bank-additional-full-copy.parquet",
                           index=False)
# read the unencrypted copy
df_copy = cse_pd.read_parquet_df("sagemaker-studio-t9sc0jup0m8", name)
print(df_copy.shape)
print(df_copy.equals(df))

cse_cmk_us = "arn:aws:kms:us-east-1:299691842772:alias/cse-us"
name = cse_pd.write_parquet_df(df, "sagemaker-studio-t9sc0jup0m8",
                           "bank-additional/bank-additional-full-copy-encrypted.parquet",
                           cmk_id=cse_cmk_us, index=False)
df_copy_enc = cse_pd.read_parquet_df("sagemaker-studio-t9sc0jup0m8", name, cmk_id=cse_cmk_us)
print(df_copy_enc.shape)
print(df_copy_enc.equals(df))
