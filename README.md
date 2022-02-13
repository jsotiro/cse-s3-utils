# CSE Client utilities for S3 Data Analytics 

AWS is a great platform for data analytics and Machine Learning but it does not support out of the box S3 Client Side Encryption (CSE) for Python. 
For details on which languages are supported see  https://docs.aws.amazon.com/general/latest/gr/aws_sdk_cryptography.html

Encrypting and decrypting S3 objects with CSE can also be done using the [aiboto3 library](https://github.com/terrycain/aioboto3) which is recommended for server-side production applications.

However, the CSE operation in aiboto3 is tightly coupled within the library's async operations. This is not always convenient for simple integration purposes, e.g. from a Jupyter notebook. 

The library focuses on data in memory and not uploading/downloading files.  The re-implementation simplifies the code and makes it easier to use alongside boto3. It also adds enhancements such as automatic translation of Key aliases to full key arns in the metadata so that it supports key rotation.

Since the main driver for this work was to read/write CSE files in Jupyter notebooks, there is also a simplified  utility to read and write client-side csv and encrypted paquet files from/into Panda dataframes.

## prerequisites and setup
The utilities assume Python 3.x
They also depend on boto3, pandas, pyarrow, and cryptography
To install the dependencies you can use the requirements.txt file 


## s3_cse_client

A simple utility to
- encrypt data in memory using KMS CMK for the datakey and store them to an S3 bucket. 
- read a cse encrypted S3 object, decrypt it in memory and return it as bytes
- read the metadata and check if a file is encrypted or return the metadata
- override the CMK in metadata if there is a need for decryption
- read/write decrypted data on top of boto3  
- use a global CSEPerformanceCounter class to log times for each operation (read, write, head) alongside with other metadata (filename, cse status, file extension) 

## cse_pandas
a simplified layer on top of s3_cse_client to 
- pandas **read_csv**/**write_csv** and **read_parquet**/**write_parquet** methods with the same signature but **with the addition of the bucket and object key parameters as well as an optional cms_id** which when specified will store the dataframe with cse-kms;when reading the libraries will automatically use the cmk id found in the object's metadata. However, this can be over-ridden by supplying the cmk_id parameter which will be used instead. This can be useful in manual key rotation scenarios
- dataframe facade to s3 object metadata
- dataframe facade to the performance counters
- a summary utility method to produce summary by operation for CSE vs NO-CSE suitable for charting the results

## Samples (WIP)
The cse_dataframes notebook shows how to use  cse_pandas and how to benchmark operations.

