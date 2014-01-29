from contracts import contract

from conf_tools.utils import check_is_in
from decent_logs import WithInternalLog
from hdflog import logger
from hdflog.tables_cache import tc_close
import numpy as np

from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_open_for_reading


__all__ = ['PGHDFLogReader', 'check_is_procgraph_log']


class PGHDFLogReader(WithInternalLog):

    def __init__(self, filename):
        self.filename = filename
        
    def _open_file(self, filename):
        hf = tc_open_for_reading(filename)
        check_is_procgraph_log(hf)

        log_group = hf.root._f_getChild(PROCGRAPH_LOG_GROUP)
        # todo: make sure we get the order
        all_signals = list(log_group._v_children)

        # self.debug('Log has signals: %s' % self.all_signals)
        
        # signal -> table
        signal2table = {}
        
        # signal -> index in the table (or None)
        signal2index = {}

        for signal in all_signals:
            signal2table[signal] = log_group._f_getChild(signal)
        
            # XXX?
            if len(signal2table[signal]) > 0:
                signal2index[signal] = 0
            else:
                signal2index[signal] = None

        return hf, all_signals, signal2table, signal2index

    def get_signal_dtype(self, signal):
        hf, all_signals, signal2table, signal2index = self._open_file(self.filename)
        
        if not signal in all_signals:
            raise ValueError(signal)
        table = signal2table[signal]
        value0 = table[0]['value']
        dtype = np.dtype((value0.dtype, value0.shape))
        
        tc_close(hf)
        return dtype
    
    def get_signal_bounds(self, signal):
        hf, all_signals, signal2table, signal2index = self._open_file(self.filename)
        
        if not signal in all_signals:
            raise ValueError(signal)
        table = signal2table[signal]
        a = table[0]['time']
        b = table[-1]['time']
        
        tc_close(hf)
          
        return a, b

    @contract(returns='list(str)')
    def get_all_signals(self):
        hf, all_signals, signal2table, signal2index = self._open_file(self.filename)
        tc_close(hf)
        return all_signals
    
    def read_signal(self, signal, start=None, stop=None):
        """ yields timestamp, (signal, value) """
        hf, all_signals, signal2table, signal2index = self._open_file(self.filename)
                
        check_is_in('signal', signal, signal2table)
            
        table = signal2table[signal]
        
        # timestamps = table[:]['time']
        timestamps = [row['time'] for row in table.iterrows()]
        
        if start is None:
            i1 = 0
            start = timestamps[0]
        else:
            i1 = np.searchsorted(timestamps, start)
        if stop is None:
            i2 = len(timestamps)
            stop = timestamps[-1]
        else:
            i2 = np.searchsorted(timestamps, stop)
            
        # self.info('requested %s - %s' % (start, stop))
        # self.info('reading %s - %s of %s' % (i1, i2, len(timestamps)))
        
        for row in table[i1:i2]:
            timestamp = row['time']
            value = row['value']
            # print('reading %r' % timestamp)
            if not (start <= timestamp <= stop):
                msg = 'Weird timestmap %s <= %s <= %s ' % (start, timestamp, stop)
                msg += '\n i1: %d i2: %d' % (i1, i2)
                logger.error(msg)
                continue
                # raise ValueError(msg) 
            yield timestamp, (signal, np.array(value))
            
        tc_close(hf)

def check_is_procgraph_log(hf):
    if not PROCGRAPH_LOG_GROUP in hf.root:
        raise Exception('File %r does not appear to be a procgraph HDF'
                        ' log: %r' % (hf.filename, hf))
        
        
        
