from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_close, tc_open_for_reading
from conf_tools.utils import check_is_in
from contracts import contract
from decent_logs import WithInternalLog
import numpy as np
import zlib


__all__ = [
    'PGHDFLogReader', 
    'check_is_procgraph_log',
]


class PGHDFLogReader(WithInternalLog):

    def __init__(self, filename):
        self.filename = filename
        
    def _open_file(self, filename):
        hf = tc_open_for_reading(filename)
        check_is_procgraph_log(hf)

        self.log_group = hf.root._f_getChild(PROCGRAPH_LOG_GROUP)
        # todo: make sure we get the order
        all_tables = list(self.log_group._v_children)

        # self.debug('Log has signals: %s' % self.all_signals)
        
        # signal -> table
        signal2table = {}
        
        # signal -> index in the table (or None)
        signal2index = {}

        for signal in all_tables:
            table = self.log_group._f_getChild(signal)
            if not 'hdflog_type' in table._v_attrs:
                tt = 'regular'
            else:
                tt = table._v_attrs['hdflog_type']
                
            if tt in [ 'vlstring_data', 'vlstring_data_yaml_gz']:
                continue
            
            signal2table[signal] = table 
        
            # XXX?
            if len(signal2table[signal]) > 0:
                signal2index[signal] = 0
            else:
                signal2index[signal] = None

        return hf, list(signal2index), signal2table, signal2index

    def get_signal_dtype(self, signal):
        hf, all_signals, signal2table, _ = self._open_file(self.filename)
        
        if not signal in all_signals:
            raise ValueError(signal)
        
        table = signal2table[signal]
        
        if not 'hdflog_type' in table._v_attrs:
            tt = 'regular'
        else:
            tt = table._v_attrs['hdflog_type']
                    
        if tt == 'regular':
            value0 = table[0]['value']
            dtype = np.dtype((value0.dtype, value0.shape))
        elif tt == 'vlstring':
            dtype = str
        elif tt == 'vlstring_data':
            msg = 'This should not happen.'
            raise Exception(msg)
        elif tt == 'string':
            dtype = str
        else:
            msg = 'Invalid col type %s' % tt
            raise ValueError(msg)
            
        tc_close(hf)
        return dtype
    
    def get_signal_bounds(self, signal):
        hf, all_signals, signal2table, _ = self._open_file(self.filename)
        
        if not signal in all_signals:
            raise ValueError(signal)
        table = signal2table[signal]
        a = table[0]['time']
        b = table[-1]['time']
        
        tc_close(hf)
          
        return a, b

    @contract(returns='list(str)')
    def get_all_signals(self):
        hf, all_signals, _, _ = self._open_file(self.filename)
        tc_close(hf)
        return all_signals
    
    def read_signal(self, signal, start=None, stop=None):
        """ yields timestamp, (signal, value) """
        hf, _, signal2table, _ = self._open_file(self.filename)
                
        check_is_in('signal', signal, signal2table)
            
        table = signal2table[signal]
        
        # timestamps = table[:]['time']
        timestamps = [row['time'] for row in table.iterrows()]
        
        if len(timestamps) == 0:
            msg = 'cannot read table %s' % table
            raise Exception(msg)
        
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

        if not 'hdflog_type' in table._v_attrs:
            tt = 'regular'
        else:
            tt = table._v_attrs['hdflog_type']
            
                
        if tt == 'vlstring':
            table_data_name = '%s_data' % signal
            table_data = self.log_group._f_getChild(table_data_name)
            ttd =  table_data._v_attrs['hdflog_type']
            assert ttd in ['vlstring_data', 'vlstring_data_yaml_gz'], ttd 

        times = np.array(table[:]['time']) 
        times = times-times[0]
        old_ts = None
        for i in range(i1,i2):

            timestamp = table[i]['time']
            
            if not (start <= timestamp <= stop):
                msg = 'Weird timestmap %s <= %s <= %s ' % (start, timestamp, stop)
                msg += '\n i1: %d i2: %d' % (i1, i2)
                self.error(msg)
                old_ts = timestamp
                continue
                # raise ValueError(msg) 

            if tt == 'vlstring':
                if ttd == 'vlstring_data':
                    value = table_data[i]
                elif ttd == 'vlstring_data_yaml_gz':
                    extra_string =  str(table_data[i])
                    value = decompress_yaml(extra_string)

                else:
                    assert False
            else:
                value = table[i]['value']    
                
            if old_ts is not None:
                delta = timestamp - old_ts
                assert delta > 0
            yield timestamp, (signal, value)
            old_ts = timestamp
            
        tc_close(hf)

def check_is_procgraph_log(hf):
    if not PROCGRAPH_LOG_GROUP in hf.root:
        raise Exception('File %r does not appear to be a procgraph HDF'
                        ' log: %r' % (hf.filename, hf))
        
        
def decompress_yaml(s):
    from hdflog.hdflogwriter import yaml_load
    extra_string = zlib.decompress(s)
    extra = yaml_load(extra_string)
#     if not isinstance(extra, dict):
#         msg = ('Expected to deserialize a dict, obtained %r' 
#                % describe_type(extra))
#         raise Exception(msg)
    return extra
