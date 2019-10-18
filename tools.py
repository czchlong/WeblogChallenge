import datetime
import gzip
import os
import re
import shutil
import zipfile

import numpy as np
import pandas as pd


ELBREGEX = r"([^ ]*) ([^ ]*) ([^ ]*):(\d*) ([^ ]*)[:-](\d*) ([-.\d]*) ([-.\d]*) ([-.\d]*) ([-\d]*) ([-\d]*) ([-\d]*) ([-\d]*) \"([^ ]*) ([^ ]*) ([^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([\w.-]*)"

ELBHEADERS = [
    "timestamp",
    "elb",
    "client_ip",
    "client_port",
    "backend_ip",
    "backend_port",
    "request_processing_time",
    "backend_processing_time",
    "response_processing_time",
    "elb_status_code",
    "backend_status_code",
    "received_bytes",
    "sent_bytes",
    "request_verb",
    "request_url",
    "request_protocol",
    "user_agent",
    "ssl_cipher",
    "ssl_protocol"
]

_filename = '2015_07_22_mktplace_shop_web_log_sample.log'


r"""Extracts the ELB log file from the data dir to local dir if not already present.

Args:
    filename (string): full filename of the log file

"""
def extract_file(filename):
    if not os.path.isfile(filename):
        with gzip.open(f'data/{_filename}.gz', 'rb') as f_in:
            with open(f'{_filename}', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

                
r"""Use the predefined ELB regex to match and parse a line into a dict.

Args:
    line (string): a line from the ELB log file

"""         
def parse_elb(line):
    matches = re.search(ELBREGEX, line)
    parsed = {}
    if matches:
        for i, h in enumerate(ELBHEADERS):
            parsed[h] = matches.group(i + 1)
    else:
        return None
        
    return parsed


r"""Constructs sessions from the raw DataFrame created from the parsed ELB log lines.

Args:
    df (pandas.DataFrame): a pandas DataFrame constructed from the parsed ELB log file lines

"""
def construct_sessions(df):
    df = df.sort_values(['timestamp'])
    df['diff'] = df.groupby('client_ip')['timestamp'].diff()
    session_df = df[['timestamp','client_ip','diff','request_url']]
    session_df = session_df.fillna(pd.Timedelta('0 days'))
    session_df['is_new_session'] = session_df['diff'].apply(lambda x: x >= datetime.timedelta(minutes=15)).astype(int)
    session_df['session_count'] = session_df.groupby('client_ip')['is_new_session'].cumsum()
    session_df['session_id'] = session_df['client_ip'].map(str) + "_" + session_df['session_count'].map(str)
    
    return session_df


r"""Constructs session lengths on a sessionized DataFrame.

Args:
    df (pandas.DataFrame): a pandas DataFrame that has is sessionized ie. contains the session_id column

"""
def build_session_lengths(df):
    df['session_diff'] = df.groupby('session_id')['timestamp'].diff()
    df = df.fillna(pd.Timedelta('0 days'))
    df['session_length'] = df.groupby('session_id')['session_diff'].apply(lambda x: x.cumsum())
    
    return df


r"""Builds the total/mean/stddev columns for session times per client_ip.

Args:
    df (pandas.DataFrame): a pandas DataFrame that contains session lengths

"""
def build_stats(df):
    df = df.groupby('session_id').tail(1)
    df = df[['client_ip','session_id','session_count','session_diff','session_length']]
    
    total = _total_session_len(df)
    mean = _mean_session_len(df)
    std = _stddev_session_len(df)
    df = df.groupby("client_ip").tail(1)
    df = df.sort_values('client_ip')
    df.reset_index(drop=True,inplace=True)
    
    merged = pd.concat([df,mean,std,total],axis=1)

    return merged


r"""Constructs a column that contains unique URL counts per session.

Args:
    df (pandas.DataFrame): a pandas DataFrame that has is sessionized ie. contains the session_id column

"""
def build_url_stats(df):
    url_counts = df.groupby('session_id')['request_url'].nunique()
    unique_url = df.groupby("session_id").tail(1)
    unique_url = unique_url.sort_values('session_id')
    url_counts = url_counts.rename(None).rename('url_counts')
    unique_url.reset_index(drop=True,inplace=True)
    url_counts.reset_index(drop=True,inplace=True)
    merged_url = pd.concat([unique_url,url_counts],axis=1)
    
    return merged_url
    

def _total_session_len(df):
    totallen = df.groupby("client_ip")['session_length'].sum()
    totallen = totallen.rename(None).rename('total')
    totallen.reset_index(drop=True, inplace=True)
    
    return totallen


def _mean_session_len(df):
    meanlen = df.groupby("client_ip")['session_length'].mean(numeric_only=False)
    meanlen = meanlen.rename(None).rename('mean')
    meanlen.reset_index(drop=True, inplace=True)
    
    return meanlen


def _stddev_session_len(df):
    stdlen = pd.to_timedelta(df.groupby('client_ip')['session_length'].std())
    stdlen = stdlen.rename(None).rename('std')
    stdlen.reset_index(drop=True, inplace=True)
    
    return stdlen
