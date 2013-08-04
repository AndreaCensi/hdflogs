from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_open_for_reading
from contracts import contract
from decent_logs import WithInternalLog


__all__ = ['PGHDFLogReader', 'check_is_procgraph_log']


class PGHDFLogReader(WithInternalLog):

    def __init__(self, filename):
        self.hf = tc_open_for_reading(filename)
        check_is_procgraph_log(self.hf)

        self.log_group = self.hf.root._f_getChild(PROCGRAPH_LOG_GROUP)
        # todo: make sure we get the order
        self.all_signals = list(self.log_group._v_children)

        self.debug('Log has signals: %s' % self.all_signals)
        
        # signal -> table
        self.signal2table = {}
        
        # signal -> index in the table (or None)
        self.signal2index = {}

        for signal in self.all_signals:
            self.signal2table[signal] = self.log_group._f_getChild(signal)
        
            # XXX?
            if len(self.signal2table[signal]) > 0:
                self.signal2index[signal] = 0
            else:
                self.signal2index[signal] = None

    def get_signal_dtype(self, signal):
        if not signal in self.all_signals:
            raise ValueError(signal)
        table = self.signal2table[signal]
        dtype = table[0]['value'].dtype
        return dtype
    
    def get_signal_bounds(self, signal):
        if not signal in self.all_signals:
            raise ValueError(signal)
        table = self.signal2table[signal]
        a = table[0]['time']
        b = table[-1]['time']  
        return a, b

    @contract(returns='list(str)')
    def get_all_signals(self):
        return self.all_signals
    
    def read_signal(self, signal):
        """ yields timestamp, (signal, value) """
        table = self.signal2table[signal]
        for row in table:
            timestamp = row['time']
            value = row['value']
            yield timestamp, (signal, value)
    

def check_is_procgraph_log(hf):
    if not PROCGRAPH_LOG_GROUP in hf.root:
        raise Exception('File %r does not appear to be a procgraph HDF'
                        ' log: %r' % (hf.filename, hf))
        
        
        