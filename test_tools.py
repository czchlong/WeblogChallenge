import numpy as np
import pandas as pd
import unittest

from pandas.testing import assert_series_equal

from tools import *


_filename = '2015_07_22_mktplace_shop_web_log_sample.log'


def _get_control():
    with open(_filename) as fin:
        lines = fin.readlines()
    parsed_entries = [parse_elb(l) for l in lines]
    batchdf = pd.DataFrame(parsed_entries, columns=ELBHEADERS)
    batchdf['timestamp'] = pd.to_datetime(batchdf['timestamp'])
    batchdf = batchdf.sort_values(['timestamp'])
    
    return batchdf[batchdf['client_ip'] == '1.186.180.183']


_control = _get_control()
_control_sessions = construct_sessions(_control)
_control_lengths = build_session_lengths(_control_sessions)
_control_stats = build_stats(_control_lengths)
_control_url = build_url_stats(_control_sessions)


class TestFunc(unittest.TestCase):

    def test_elbregex(self):
        extract_file(_filename)
        with open(_filename) as fin:
            lines = fin.readlines()
        
        for l in lines:
            res = parse_elb(l)
            
            assert(res != None)

    def test_construct_sessions(self):
        session_count = _control_sessions.session_count.max()
        
        assert(session_count == 1)
        
        session_ids = _control_sessions.session_id.unique()
        
        assert(set(session_ids) == set(['1.186.180.183_0', '1.186.180.183_1']))

    def test_build_session_lengths(self):
        diffs = pd.Series(['00:00:00','00:00:00','00:00:03.369402','00:00:11.156182','00:00:10.865919'])
        timedeltas = pd.to_timedelta(diffs)
        
        expected = timedeltas.values
        actual = _control_lengths.session_diff.values
        
        assert(np.array_equal(expected, actual))
        
        expected = timedeltas.cumsum().values
        actual = _control_lengths.session_length.values
        
        assert(np.array_equal(expected, actual))
        
        
    def test_build_stats(self):
        total_len = pd.to_timedelta(pd.Series(['00:00:25.391503']))

        assert(np.array_equal(_control_stats['total'].values, total_len.values))


    def test_build_url(self):
        assert(_control_url['url_counts'].tolist() == [1,4])
        
    
if __name__ == '__main__':
    unittest.main()
