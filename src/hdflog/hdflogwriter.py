from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_close, tc_open_for_writing
from contracts import contract
from decent_logs import WithInternalLog
import numpy as np
import os
from contracts.utils import check_isinstance
import tables
from tables.description import IsDescription


__all__ = [
    'PGHDFLogWriter',
]


class PGHDFLogWriter(WithInternalLog):
    
    """ Writes a log to an HDF file. The entries should map to numpy values. """
    
    def __init__(self, filename, compress=True, complevel=9, complib='zlib'):
        self.filename = filename
        self.compress = compress
        self.complevel = complevel
        self.complib = complib
        

        dirname = os.path.dirname(filename)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
            
        self.tmp_filename = filename + '.active'
        # self.info('Writing to file %r.' % self.tmp_filename)
        self.hf = tc_open_for_writing(self.tmp_filename)

        self.group = self.hf.createGroup(self.hf.root, PROCGRAPH_LOG_GROUP)
        # TODO: add meta info

        # signal name -> table in hdf file
        self.signal2table = {}
        # signal name -> last timestamp written
        self.signal2timestamp = {}

        self.closed = False
        
    @contract(signal='str', timestamp='float')
    def _check_good_timestamp(self, signal, timestamp):
        self.info('check_good: %.4f %s' % (timestamp, signal))
        # also check that we didn't already log this instant
        if not signal in self.signal2timestamp:
            self.signal2timestamp[signal] = timestamp
            return
        
        old = self.signal2timestamp[signal]
        
        delta = timestamp - old
        
        if not delta > 0:
            
            msg = ('Signal %s has wrong ts sequence: %g -> %g (delta = %g)' % 
                   (signal, timestamp, old, delta)) 
            raise ValueError(msg)
        
        self.signal2timestamp[signal] = timestamp


    def get_hf(self):
        return self.hf
    
    def get_group(self):
        return self.group

    @contract(timestamp='float', signal='str', value='array')
    def log_signal(self, timestamp, signal, value):
        check_isinstance(timestamp, float)
        check_isinstance(signal, str) 
        check_isinstance(value, np.ndarray)
        self._check_good_timestamp(signal, timestamp)
        
        # check that we have the table for this signal
        table_dtype = [('time', 'float64'),
                       ('value', value.dtype, value.shape)]

        table_dtype = np.dtype(table_dtype)

        # TODO: check that the dtype is consistnet

        if not signal in self.signal2table:
            # a bit of compression. zlib is standard for hdf5
            # fletcher32 writes by entry rather than by rows
            if self.compress:
                filters = tables.Filters(
                            complevel=self.complevel,
                            complib=self.complib,
                            fletcher32=True)
            else:
                filters = tables.Filters(fletcher32=True)

            try:
                table = self.hf.createTable(
                        where=self.group,
                        name=signal,
                        description=table_dtype,
                        # expectedrows=10000, # large guess
                        byteorder='little',
                        filters=filters
                    )
            except NotImplementedError as e:
                msg = 'Could not create table with dtype %r: %s' % (table_dtype, e)
                # raise BadInput(msg, self, input_signal=signal)
                raise ValueError(msg)

            print('Created table %r' % table)
            self.signal2table[signal] = table
            self.signal2table[signal]._v_attrs['hdflog_type'] = 'regular'

        else:
            table = self.signal2table[signal]


        row = np.ndarray(shape=(1,), dtype=table_dtype)
        row[0]['time'] = timestamp
        if value.size == 1:
            row[0]['value'] = value
        else:
            row[0]['value'][:] = value
        # row[0]['value'] = value  <--- gives memory error
        table.append(row)

    @contract(timestamp='float', signal='str', value='str')
    def log_short_string(self, timestamp, signal, value, itemsize=256):
        """ Logs a variable-length string. """
        check_isinstance(timestamp, float)
        check_isinstance(signal, str) 
        check_isinstance(value, str)
        self._check_good_timestamp(signal, timestamp)
        
        table_dtype = [('time', 'float64'),
                       ('value', (str, itemsize)),
                       ]
        if not signal in self.signal2table:
                
            self.info('table: %s' % table_dtype)

            self.signal2table[signal] = self.hf.createTable(
                        where=self.group,
                        name=signal,
                        description=StringRow)
    
            self.signal2table[signal]._v_attrs['hdflog_type'] = 'string'

        table = self.signal2table[signal]
        
        if True:
            
            table.append([(timestamp, value)])
            
        elif True:
            row = table.row
            crow = row.nrow
            
            row['value'] = value
            row['time'] = timestamp
            self.error('short_string: %.5f %s' % (timestamp, value))
            row.append()
            nrow = row.nrow
            self.info('current row: %s %s of %s'  % (crow, nrow, table.nrows))
            assert nrow == crow + 1
             
        else:
            row = table.row
            row.append()
            
            row = np.ndarray(shape=(), dtype=table_dtype)
            row['time'] = timestamp
            row['value'] = value
            print row
            table.append(row)
            
        table.flush()
        
        last = table[len(table)-1]
        self.error('size %s last %s' % (len(table), last))
        if not last['value'] == value or not last['time'] == timestamp:
            msg = 'Could not commit table %r to file.' % (signal)
            msg += '\ntable nrows: %s' % len(table)
            msg += '\ntimestamp: %.5f value: %r ' % (timestamp, value)
            msg += '\n last row: %s' % last
            raise Exception(msg)
        
    
    @contract(timestamp='float', signal='str', value='str')
    def log_large_string(self, timestamp, signal, value):
        """ Logs a variable-length string. """
        check_isinstance(timestamp, float)
        check_isinstance(signal, str) 
        check_isinstance(value, str)
        self._check_good_timestamp(signal, timestamp)
        
        table_data_name = '%s_data' % signal
        table_index_name = signal

        if not signal in self.signal2table:
            # we create '<signal>' of type (float, len()) and '<signal>_data' of type vlatom
            
            filters_text = tables.Filters(complevel=1, shuffle=False,
                                          fletcher32=False, complib='zlib')
            
            table_data =  self.hf.createVLArray(self.group, table_data_name,
                                                tables.VLStringAtom(),
                                                filters=filters_text)
            table_index = self.hf.createTable(where=self.group,
                                              name=table_index_name,
                                              description=IndexRow)
            self.info(str(table_index))
            table_index._v_attrs['hdflog_type'] = 'vlstring'
            table_data._v_attrs['hdflog_type'] = 'vlstring_data'

            self.signal2table[signal] = table_index

        table_index = self.signal2table[signal]
        table_data = self.group._v_children[table_data_name]

        print(table_index)    
        row = table_index.row
        row['size_of_value'][0] = len(value)
        row['time'] = timestamp
        row.append()
        
        table_data.append(value)
    
    @contract(timestamp='float', signal='str', value='str|dict|list|int|float')
    def log_compressed_yaml(self, timestamp, signal, value):
        """ Logs a  tructure by converting to YAML and then compressing. """
        #self.info('log_compressed_yaml: %.5f %s' % (timestamp, signal))
        check_isinstance(timestamp, float)
        check_isinstance(signal, str) 
        #check_isinstance(value, str)
        
        yaml = yaml_dump(value)
        gz = compress(yaml)
        self.log_large_string(timestamp, signal, gz)
        table_data_name = '%s_data' % signal
        table_data = self.group._v_children[table_data_name]
        table_data._v_attrs['hdflog_type'] = 'vlstring_data_yaml_gz'

    def finish(self):     
        if self.closed: # robust to second time
            return   
        self.closed = True
        tc_close(self.hf)
        if os.path.exists(self.filename):
            os.unlink(self.filename)
        os.rename(self.tmp_filename, self.filename)


def compress(s):
    ''' Compresses using only one thread. '''
    import zlib
    encoder = zlib.compressobj()
    data = encoder.compress(s)
    data = data + encoder.flush()
    return data
        


try:
    import yaml
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError as e:
    msg = ('Could not load C YAML reader. I can continue but '
          'everything will be slow. (%s)' % e)
    print(msg)
    from yaml import Loader, Dumper


@contract(yaml_string='str')
def yaml_load(yaml_string):
    try:
        return yaml.load(yaml_string, Loader=Loader)
    except KeyboardInterrupt:
        raise
    except:
#         logger.error('Could not deserialize YAML')
#         dump_emergency_string(yaml_string)
        raise
    
@contract(returns='str')
def yaml_dump(ob):
#     check_pure_structure(ob)
    string = yaml.dump(ob, Dumper=Dumper)
    if '!python/object' in string:
#         dump_emergency_string(string)
        msg = 'Invalid YAML produced'
        raise ValueError(msg)
    return string

from tables import StringCol, Int32Col, Float64Col  # @UnresolvedImport
class StringRow(IsDescription):
    time  = Float64Col()
    value  = StringCol(256)   # 16-character String

class IndexRow(IsDescription):
    time  = Float64Col()
    size_of_value  = Int32Col(256)   # 16-character String
