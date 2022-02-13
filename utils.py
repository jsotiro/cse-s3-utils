import pandas as pd
import numpy as np


def is_encrypted(metadata):
    return 'x-amz-key' in metadata or 'x-amz-key-v2' in metadata


def format_time_elapsed(t):
    mins = 0
    hours = 0

    secs = t * 100 // 100

    if secs > 59:
        mins = secs // 60
        mins_remainder = secs % 60
        secs = mins_remainder
        if mins > 59:
            hours = mins // 60
            hours_remainder = mins % 60
            mins = hours_remainder
    # return '{} hour(s) {} min(s) {} sec(s)'.format(hours,mins,secs)
    return f"{t} seconds"


def counters_summary_df(counters_df, index_col, label_col, values_col):
    simple_df = counters_df[[index_col, label_col, values_col]]
    results_df = pd.DataFrame()
    group_names = np.unique(simple_df[label_col].values)
    for value in group_names:
        subset_df = simple_df[simple_df[label_col] == value].copy()
        subset_df.rename(columns={values_col : value}, inplace=True)
        subset_df.set_index(index_col, inplace=True)
        if results_df.empty:
            results_df = subset_df
        else:
            results_df[value] = subset_df[value]
    del results_df[label_col]
    return results_df
