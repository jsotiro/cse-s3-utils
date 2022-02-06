# CSE Client utilities for S3 

## s3_cse_client
A simple utility to 
- encrypt data in memory using KMS for the data and store them to an S3 bucket 
- read a cse encrypted S3 object, decrypt it in memory and return it as bytes

Encrypting and decrypting S3 objects with CSE can also be done using aiboto3 which is recommended for production purposes. However the cse operation in aiboto is coupled within he libaries async operations that is not always convenient for simple integration purposes, eg from a Jypyter notebook. This implementation is a  simple and straightforward   implementation on top of boto3.     
The implementation is based on similar work which focuses on files istead of memories. In this implemenation we use memory mapped files (BufferIO) to achieve the same functionality

Note that we use PyCryptodome

## encrypted_df





