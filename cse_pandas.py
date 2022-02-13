"""Utilities for reading and writing dataframes using S3 CSE."""

import pandas as pd
import io
from cse_performance_counters import CsePerformanceCounters
import utils
from s3_cse_client import S3CseClient

cse_perf_counters = CsePerformanceCounters()


def read_csv_df(bucket, filename, cmk_id=None, sep=",", header='infer', names=None,
                index_col=None, usecols=None, squeeze=None, prefix=None, mangle_dupe_cols=True,
                dtype=None, converters=None, true_values=None, false_values=None,
                skipinitialspace=False, skiprows=None, skipfooter=0,
                nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False,
                skip_blank_lines=True, parse_dates=None, infer_datetime_format=False, keep_date_col=False,
                date_parser=None, dayfirst=False, cache_dates=True, iterator=False, chunksize=None, compression='infer',
                thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=0, doublequote=True,
                escapechar=None, comment=None, encoding=None, encoding_errors='strict',
                dialect=None, error_bad_lines=None, warn_bad_lines=None, on_bad_lines=None,
                delim_whitespace=False, low_memory=True, memory_map=False, float_precision=None):
    s3 = S3CseClient(cmk_id, perf_counters=cse_perf_counters)
    data = s3.read(bucket, filename)
    f_in = io.BytesIO()
    f_in.write(data)
    f_in.seek(0)
    # create a new data frame from the  in mem buffer
    # allow the caller to pass through parameters
    new_df = pd.read_csv(f_in,
                         sep=sep, header=header, names=names,
                         index_col=index_col, usecols=usecols, squeeze=squeeze, prefix=prefix,
                         mangle_dupe_cols=mangle_dupe_cols,
                         dtype=dtype, converters=converters, true_values=true_values, false_values=false_values,
                         skipinitialspace=skipinitialspace, skiprows=skiprows, skipfooter=skipfooter,
                         nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter,
                         verbose=verbose,
                         skip_blank_lines=skip_blank_lines, parse_dates=parse_dates, infer_datetime_format=parse_dates,
                         keep_date_col=keep_date_col,
                         date_parser=date_parser, dayfirst=dayfirst, cache_dates=cache_dates, iterator=chunksize,
                         chunksize=chunksize, compression=compression,
                         thousands=thousands, decimal=decimal, lineterminator=lineterminator, quotechar=quotechar,
                         quoting=quoting, doublequote=doublequote,
                         escapechar=escapechar, comment=comment, encoding=encoding, encoding_errors=encoding_errors,
                         dialect=dialect, error_bad_lines=error_bad_lines, warn_bad_lines=warn_bad_lines,
                         on_bad_lines=on_bad_lines,
                         delim_whitespace=delim_whitespace, low_memory=low_memory, memory_map=memory_map,
                         float_precision=float_precision)
    return new_df


def write_csv_df(df, bucket, filename,
                 cmk_id=None,
                 sep=',', na_rep='', float_format=None, columns=None, header=True, index=True,
                 index_label=None, encoding=None, quoting=None, quotechar='"', line_terminator=None, chunksize=None,
                 date_format=None, doublequote=True, escapechar=None, decimal='.', errors='strict'):
    s3 = S3CseClient(cmk_id, perf_counters=cse_perf_counters)
    f_out = io.BytesIO()
    df.to_csv(f_out,
              sep=sep, na_rep=na_rep, float_format=float_format, columns=columns,
              header=header, index=index, index_label=index_label, encoding=encoding,
              quoting=quoting, quotechar=quotechar, line_terminator=line_terminator,
              chunksize=chunksize, date_format=date_format, doublequote=doublequote,
              escapechar=escapechar, decimal=decimal, errors=errors)
    f_out.seek(0)
    df_data = f_out.read()
    object_key = filename
    s3.write(bucket, object_key, df_data)
    return object_key


def metadata_df(metadata):
    df = pd.DataFrame(list(metadata.items()), columns=['Key', 'Value'])
    return df


def is_encrypted(metadata):
    return utils.is_encrypted(metadata)


def dict_to_df(metadata):
    return pd.DataFrame(list(metadata.items()), columns=['Key', 'Value'])


def file_metadata(bucket,
                  filename,
                  extended=False):
    s3 = S3CseClient("", perf_counters=cse_perf_counters)
    metadata = s3.get_metadata(bucket, filename, extended=extended)
    return metadata


def read_parquet_df(bucket,
                    filename,
                    cmk_id=None,
                    columns=None,
                    use_nullable_dtypes=None,
                    **kwargs):
    s3 = S3CseClient(cmk_id, perf_counters=cse_perf_counters)
    data = s3.read(bucket, filename)
    f_in = io.BytesIO()
    f_in.write(data)
    # create a new data frame from the  in mem buffer
    new_df = pd.read_parquet(f_in, columns=columns, use_nullable_dtypes=use_nullable_dtypes, **kwargs)
    return new_df


def write_parquet_df(df, bucket, filename,
                     cmk_id=None,
                     compression='snappy', index=False):
    s3 = S3CseClient(cmk_id, perf_counters=cse_perf_counters)
    f_out = io.BytesIO()
    df.to_parquet(path=f_out, engine='pyarrow', compression=compression, index=index)
    f_out.seek(0)
    parquet_data = f_out.read()
    object_key = filename
    s3.write(bucket, object_key, parquet_data)
    return object_key


def get_performance_counters():
    return pd.DataFrame(cse_perf_counters._counters)
