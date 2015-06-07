from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_close, tc_open_for_writing
from contracts import check_isinstance, contract, describe_value
from decent_logs import WithInternalLog
from tables import Float64Col, Int32Col, StringCol # @UnresolvedImport
from tables.description import IsDescription, Col, Description
import numpy as np
import os
import tables
import warnings
from hdflog import HDFLogOptions


__all__ = [
    'PGHDFLogWriter',
]


class PGHDFLogWriter(WithInternalLog):
    
    """ Writes a log to an HDF file. The entries should map to numpy values. """
    
    def __init__(self, filename, compress=True, complevel=9, complib='zlib',
                 allow_delta0='default'):
        if allow_delta0 == 'default':
            allow_delta0 = HDFLogOptions.allow_delta0
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
        # signal name -> dtype of table
        self.signal2dtype = {}
        # signal name -> last timestamp written
        self.signal2timestamp = {}

        self.closed = False
        self.allow_delta0 = allow_delta0
        
    @contract(signal='str', timestamp='float')
    def _check_good_timestamp(self, signal, timestamp, value):
        warnings.warn('timestamp disabled')
        #self.info('check_good: %.4f %s' % (timestamp, signal))
        # also check that we didn't already log this instant
        if not signal in self.signal2timestamp:
            self.signal2timestamp[signal] = timestamp
            return
        
        old = self.signal2timestamp[signal]
        
        delta = timestamp - old
        
        def format_ts(x):
            return '%20.9f' % x
        if delta < 0 or (delta==0 and not self.allow_delta0):
            msg = ('Signal %r has wrong ts sequence: %s -> %s (delta = %.5f)' % 
                   (signal, format_ts(timestamp), 
                    format_ts(old), delta))
            
            table  = self.signal2table[signal]
            last_row = table[len(table)-1]
            
            msg += '\n last_row: %s' % str(last_row)
            msg += '\n putting value %s' % describe_value(value)
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
        self._check_good_timestamp(signal, timestamp, value)

        def looks_like_array(value):
            dt = value.dtype
            if value.ndim == 1 and value.size >= 1 and ('timestamp' in dt.fields):
                return True
            else:
                # print('not array', value.ndim, value.size, dt)
                return False
            
        if looks_like_array(value):
            table, table_dtype = self._log_signal_get_table_and_dtype(signal, value[0])

            # print('looks like array: %s, %s, %s' % (signal, value.dtype, value.shape))
            n = value.size
            rows = np.ndarray(shape=(n,), dtype=table_dtype)
            rows[:]['time'] = value['timestamp']
            for i in range(n):
                rows[i]['value'] = value[i]

#             print('appending', rows)
            table.append(rows)

        else:
            table, table_dtype = self._log_signal_get_table_and_dtype(signal, value)

            row = np.ndarray(shape=(1,), dtype=table_dtype)
            row[0]['time'] = timestamp
            # print('value dtype: %s %s' % (value.dtype, value.shape))
            # print('table dtype: %s %s' % (row[0]['value'].dtype, row[0]['value'].shape) )
            if value.shape == ():
                row[0]['value'] = value
            else:
                row[0]['value'][:] = value
            table.append(row)

        # row[0]['value'] = value  <--- gives memory error

        # warnings.warn('xxx')
#         else:
#             if signal == 'events':
#                 table.append(row)
#             else:
#                 from bootstrapping_olympics.utils.warn_long_time_exc import warn_long_time
#                 with warn_long_time(max_wall_time=0.01, what="table.append(%s)" % signal, logger=None):
#                     table.append(row)

    def _log_signal_get_table_and_dtype(self, signal, value):
        """ Returns table, table_dtype """
        if signal in self.signal2table:
            table = self.signal2table[signal]
            table_dtype = self.signal2dtype[signal]
            return table, table_dtype

        else:
            table_dtype = [
                ('time', 'float64'),
                ('value', (value.dtype, value.shape))
            ]

            table_dtype = np.dtype(table_dtype)

            if value.dtype.names:
                # composite dtype
                if value.shape == (1,):
                    msg = ("Warning, this will work but it will be read back "
                           "as a different dtype---it's a limitation of PyTables.")
                    msg += '\n signal: %s' % signal
                    msg += '\n shape: %s  dtype: %s' % (value.shape, value.dtype)
                    self.warn(msg)
                if len(value.shape) == 1 and value.shape[0] > 1:
                    msg = ('Warning, this will probably fail because of PyTables '
                           'limitations with recursive arrays of len > 1.')
                    msg += '\n signal: %s' % signal
                    msg += '\n shape: %s  dtype: %s' % (value.shape, value.dtype)
                    self.warn(msg)
    
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
                descr, _ = descr_from_dtype_backported(table_dtype)
                #print('description: %s' % descr)
                table = self.hf.createTable(
                        where=self.group,
                        name=signal,
                        description=descr,
                        # expectedrows=10000, # large guess
                        byteorder='little',
                        filters=filters,
                    )
            except NotImplementedError as e:
                msg = 'Could not create table with dtype %r: %s' % (table_dtype, e)
                # raise BadInput(msg, self, input_signal=signal)
                raise ValueError(msg)

            
            self.signal2table[signal] = table
            self.signal2table[signal]._v_attrs['hdflog_type'] = 'regular'
            self.signal2dtype[signal] = table_dtype
            return table, table_dtype




    @contract(timestamp='float', signal='str', value='str')
    def log_short_string(self, timestamp, signal, value, itemsize=256):
        """ Logs a variable-length string. """
        check_isinstance(timestamp, float)
        check_isinstance(signal, str) 
        check_isinstance(value, str)
        self._check_good_timestamp(signal, timestamp, value)
        
        table_dtype = [
           ('time', 'float64'),
           ('value', (str, itemsize)),
        ]
        
        if not signal in self.signal2table:
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
            row.append()
            nrow = row.nrow
            assert nrow == crow + 1
             
        else:
            row = table.row
            row.append()
            
            row = np.ndarray(shape=(), dtype=table_dtype)
            row['time'] = timestamp
            row['value'] = value
            
            table.append(row)
            
        table.flush()
        
        last = table[len(table)-1]
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
        self._check_good_timestamp(signal, timestamp, value)
        
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
            table_index._v_attrs['hdflog_type'] = 'vlstring'
            table_data._v_attrs['hdflog_type'] = 'vlstring_data'

            self.signal2table[signal] = table_index

        table_index = self.signal2table[signal]
        table_data = self.group._v_children[table_data_name]


        row = table_index.row
        row['size_of_value'][0] = len(value)
        row['time'] = timestamp
        row.append()
        
        table_data.append(value)
    
    @contract(timestamp='float', signal='str', value='str|dict|list|int|float')
    def log_compressed_yaml(self, timestamp, signal, value):
        """ Logs a  tructure by converting to YAML and then compressing. """
        check_isinstance(timestamp, float)
        check_isinstance(signal, str)
        
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
        os.rename(self.tmp_filename, self.filename)
        assert os.path.exists(self.filename)
        assert not os.path.exists(self.tmp_filename)


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
    string = yaml.dump(ob, Dumper=Dumper)
    if '!python/object' in string:
        # dump_emergency_string(string)
        msg = 'Invalid YAML produced'
        raise ValueError(msg)
    return string

class StringRow(IsDescription):
    time  = Float64Col()
    value  = StringCol(256)   # 16-character String

class IndexRow(IsDescription):
    time  = Float64Col()
    size_of_value  = Int32Col(256)   # 16-character String


# See changeset https://github.com/PyTables/PyTables/commit/f4ba6c929d1889ba7e7d6c936b9381737b9ef2d1
def descr_from_dtype_backported(dtype_):
    """Get a description instance and byteorder from a (nested) NumPy dtype."""

    fields = {}
    fbyteorder = '|'
    for name in dtype_.names:
        dtype, pos = dtype_.fields[name][:2]
        kind = dtype.base.kind
        byteorder = dtype.base.byteorder
        if byteorder in '><=':
            if fbyteorder not in ['|', byteorder]:
                raise NotImplementedError(
                    "structured arrays with mixed byteorders "
                    "are not supported yet, sorry")
            fbyteorder = byteorder
        # Non-nested column
        if kind in 'biufSc':
            col = Col.from_dtype(dtype, pos=pos)
        # Nested column
        elif kind == 'V' and dtype.shape in [(), (1,)]:
            col, _ = descr_from_dtype_backported(dtype.base)
            col._v_pos = pos
        else:
            raise NotImplementedError(
                "structured arrays with columns with type description ``%s`` "
                "are not supported yet, sorry" % dtype)
        fields[name] = col

    return Description(fields), fbyteorder
