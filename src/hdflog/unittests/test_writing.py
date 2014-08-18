from hdflog import PGHDFLogReader, PGHDFLogWriter
from numpy.testing.utils import assert_allclose
import numpy as np
import unittest
import tempfile
import os

import functools
import nose

def expected_failure(test):
    @functools.wraps(test)
    def inner(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except Exception:
            raise nose.SkipTest
        else:
            raise AssertionError('Failure expected')
    return inner

class Tests(unittest.TestCase):
        
    def test_writing1(self):
        data = [
                (0.0, ('s1', np.array([42,43]))),
                (1.0, ('s1', np.array([44,43]))),
                (2.0, ('s1', np.array([42,45]))),
        ]
        readback = self._write_log_signal(data)
        self.assertEqual(set(readback), set(['s1']))
        
        read = readback['s1']
        assert len(read) == len(data)
        
        for i, v in enumerate(read):
            d = data[i]
            print('read[i]: %s %s %s ' % (v[1][1].dtype, v[1][1].shape, str(v)))
            print('data[i]: %s %s %s ' % (d[1][1].dtype, d[1][1].shape, str(d)))
            
            self.assertEqual(v[0], d[0])
            self.assertEqual(v[1][0], d[1][0])
            assert_allclose(v[1][1], d[1][1])
        
    #@contract(returns='dict')
    def _write_log_signal(self, data, func=PGHDFLogWriter.log_signal):
        """ writes using log_signal --- returns the data read back
            one for each signal. """
        tmp_fd, tmp_name = tempfile.mkstemp()
        os.close(tmp_fd)
        w = PGHDFLogWriter(tmp_name, compress=True, complevel=9, complib='zlib')
        signals = set()
        for t, (signal, value) in data:
            signals.add(signal)
            func(w, t, signal, value)
        w.finish()
        r = PGHDFLogReader(tmp_name)
        self.assertEqual(set(r.get_all_signals()),
                         set(signals))
        readback = {}
        for s in signals:
            readback[s] = list(r.read_signal(s, start=None, stop=None))
        return readback
         
    def test_writing2(self):
        data = [
                (0.0, ('s1', 'hello')),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', 'are')),
                (3.0, ('s1', 'you')),
        ]    
        readback = self._write_log_signal(data, func=PGHDFLogWriter.log_short_string)

        self.assertEqual(data, readback['s1'])
        
         
    
    def test_writing3(self):
        data = [
                (0.0, ('s1', 'hello')),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', 'are')),
                (3.0, ('s1', 'you')),
        ]
        
        readback = self._write_log_signal(data, 
                                          func=PGHDFLogWriter.log_large_string)
    
        self.assertEqual(data, readback['s1'])
    
    
    
    def test_writing4_yaml(self):
     
        data = [
                (0.0, ('s1', dict(data='hello'))),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', dict(hey='are'))),
                (3.0, ('s1', 'you')),
        ]
        readback = self._write_log_signal(data, 
                                  func=PGHDFLogWriter.log_compressed_yaml)
    
        self.assertEqual(data, readback['s1'])

    
    
    def test_writing5_composite(self): 
        # this fails because
        aer_raw_event_dtype = [
            ('timestamp', 'float'),
            ('x', 'int16'),
            ('y', 'int16'),
            ('sign', 'int8')
        ]
        x1 = np.zeros(shape=(), dtype=aer_raw_event_dtype)
        data = [
                (0.0, ('s1', x1)),
        ]
        
        readback = self._write_log_signal(data, 
                          func=PGHDFLogWriter.log_signal)
    
        read = readback['s1']
        
        assert read[0][1][0] == 's1'
        x2 = read[0][1][1]
        print('x1 %s %s' % (x1.shape, x1.dtype))
        print('x2 %s %s' % (x2.shape, x2.dtype))
        self.assertEqual(x2.shape, x1.shape)
        self.assertEqual(x2.dtype, x1.dtype)
     
    @expected_failure
    def test_writing5b_composite(self):
        """ This fails because of PyTables. """
        aer_raw_event_dtype = [
            ('timestamp', 'float'),
            ('x', 'int16'),
            ('y', 'int16'),
            ('sign', 'int8')
        ]
        x1 = np.zeros(shape=(1,), 
                      dtype=aer_raw_event_dtype)
        data = [
                (0.0, ('s1', x1)),
        ]

        readback = self._write_log_signal(data, 
                          func=PGHDFLogWriter.log_signal)
        
        read = readback['s1']
        assert read[0][1][0] == 's1'
        x2 = read[0][1][1]
        print('x1 %s %s' % (x1.shape, x1.dtype))
        print('x2 %s %s' % (x2.shape, x2.dtype))
        self.assertEqual(x2.shape, x1.shape)
        self.assertEqual(x2.dtype, x1.dtype)
        
    @expected_failure
    def test_writing5c_len(self):
        """ This fails because of PyTables. """
        aer_raw_event_dtype = [
            ('timestamp', 'float'),
            ('x', 'int16'),
            ('y', 'int16'),
            ('sign', 'int8')
        ]
        # note the shape = (3,)
        x1 = np.zeros(shape=(3,), 
                      dtype=aer_raw_event_dtype)
        data = [
                (0.0, ('s1', x1)),
        ]

        readback = self._write_log_signal(data, 
                          func=PGHDFLogWriter.log_signal)
        

       
    def test_writing6_1dvalues(self): 
        # this is a scalar
        x1 = np.array(1)
        data = [
                (0.0, ('s1', x1)),
        ]
        readback = self._write_log_signal(data, 
                  func=PGHDFLogWriter.log_signal)
        read = readback['s1']
        assert read[0][1][0] == 's1'
        x2 = read[0][1][1]
        self.assertEqual(x2.shape, ())
        self.assertEqual(x2.dtype, np.int64)
        

