import heapq

from hdflog import PGHDFLogReader
from rawlogs import RawSignal
from rawlogs.library import LogWithAnnotations


__all__ = ['HDFRawLog']

class HDFRawLog(LogWithAnnotations):
    
    def __init__(self, filename, annotations={}):
        LogWithAnnotations.__init__(self, annotations=annotations)
        self.filename = filename

    def _get_reader(self):
        return PGHDFLogReader(self.filename)

    def get_signals(self):
        reader = self._get_reader()
        
        signals = {}
        for s in reader.get_all_signals():
            dtype = reader.get_signal_dtype(s)
            bounds = reader.get_signal_bounds(s)
            signals[s] = HDFRawSignal(dtype=dtype, filename=self.filename,
                                      bounds=bounds)
        return signals   
    
    def get_resources(self): 
        return [self.filename]

    def read(self, topics, start=None, stop=None):
        reader = self._get_reader()
        iterators = []    
        for signal in topics:
            one = reader.read_signal(signal, start=start, stop=stop)
            iterators.append(one)
        it = heapq.merge(*tuple(iterators))
        for x in it:
            yield x


class HDFRawSignal(RawSignal):
    def __init__(self, dtype, filename, bounds):
        self.dtype = dtype
        self.filename = filename
        self.bounds = bounds        

    def get_signal_type(self):
        return self.dtype        

    def get_time_reference(self):
        return 'default'
        
    def get_resources(self):
        return self.filename
      
    def get_time_bounds(self):
        return self.bounds
    
    def get_length(self):
        a, b = self.get_time_bounds()
        return b - a
    
    
