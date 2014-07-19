from hdflog import PGHDFLogReader, PGHDFLogWriter
from numpy.testing.utils import assert_allclose
import numpy as np
import unittest


class Tests(unittest.TestCase):
        
    def test_writing1(self):
    
        filename = 'test_writing1.h5'

        w = PGHDFLogWriter(filename, compress=True, complevel=9, complib='zlib')
        data = [
                (0.0, ('s1', np.array([42,43]))),
                (1.0, ('s1', np.array([44,43]))),
                (2.0, ('s1', np.array([42,45]))),
        ]
        
        for t, (signal, value) in data:
            w.log_signal(t, signal, value)
        
        w.finish()
        
        r = PGHDFLogReader(filename)
        signals = r.get_all_signals()
        self.assertEqual(signals, ['s1'])
        
        read = list(r.read_signal('s1', start=None, stop=None))
        assert len(read) == len(data)
        
        for i, v in enumerate(read):
            d = data[i]
            self.assertEqual(v[0], d[0])
            self.assertEqual(v[1][0], d[1][0])
            assert_allclose(v[1][1], d[1][1])
        
        
    
    def test_writing2(self):
    
        filename = 'test_writing2.h5'
        w = PGHDFLogWriter(filename, compress=True, complevel=9, complib='zlib')
        
        data = [
                (0.0, ('s1', 'hello')),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', 'are')),
                (3.0, ('s1', 'you')),
        ]
        
        for t, (signal, value) in data:
            w.log_short_string(t, signal, value)
        
        w.finish()
    
    
        r = PGHDFLogReader(filename)
        signals = r.get_all_signals()
        self.assertEqual(signals, ['s1'])
        
        read = list(r.read_signal('s1', start=None, stop=None))
        self.assertEqual(data, read)
        
        
    
    def test_writing2b(self):
    
        filename = 'test_writing2b.h5'
        w = PGHDFLogWriter(filename, compress=True, complevel=9, complib='zlib')
        
        data = [
                (0.0, ('s1', 'hello')),
                (1.0, ('s1', 'hello')),
                (2.0, ('s1', 'hello')),
                (3.0, ('s1', 'hello')),
        ]
        
        for t, (signal, value) in data:
            w.log_short_string(t, signal, value)
        
        w.finish()
    
    
        r = PGHDFLogReader(filename)
        signals = r.get_all_signals()
        self.assertEqual(signals, ['s1'])
        
        read = list(r.read_signal('s1', start=None, stop=None))
        self.assertEqual(data, read)
        
    
    def test_writing3(self):
    
        filename = 'test_writing3.h5'
        w = PGHDFLogWriter(filename, compress=True, complevel=9, complib='zlib')
        
        data = [
                (0.0, ('s1', 'hello')),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', 'are')),
                (3.0, ('s1', 'you')),
        ]
        
        for t, (signal, value) in data:
            w.log_large_string(t, signal, value)
        
        w.finish()
    
    
        r = PGHDFLogReader(filename)
        signals = r.get_all_signals()
        self.assertEqual(signals, ['s1'])
        
        read = list(r.read_signal('s1', start=None, stop=None))
        self.assertEqual(data, read)
    
    def test_writing4_yaml(self):
    
        filename = 'test_writing3.h5'
        w = PGHDFLogWriter(filename, compress=True, complevel=9, complib='zlib')
        
        data = [
                (0.0, ('s1', dict(data='hello'))),
                (1.0, ('s1', 'how')),
                (2.0, ('s1', dict(hey='are'))),
                (3.0, ('s1', 'you')),
        ]
        
        for t, (signal, value) in data:
            w.log_compressed_yaml(t, signal, value)
        
        w.finish()
    
    
        r = PGHDFLogReader(filename)
        signals = r.get_all_signals()
        self.assertEqual(signals, ['s1'])
        
        read = list(r.read_signal('s1', start=None, stop=None))
        self.assertEqual(data, read)
        
        
        
        
        