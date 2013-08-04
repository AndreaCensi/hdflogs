from . import PROCGRAPH_LOG_GROUP
from .tables_cache import tc_close, tc_open_for_writing
from contracts import contract
from decent_logs import WithInternalLog
import numpy as np
import os


__all__ = ['PGHDFLogWriter']


class PGHDFLogWriter(WithInternalLog):
    
    """ Writes a log to an HDF file. The entries should map to numpy values. """
    
    def __init__(self, filename, compress=True, complevel=9, complib='zlib'):
        self.filename = filename
        self.compress = compress
        self.complevel = complevel
        self.complib = complib
        

        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
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

    @contract(timestamp='float', signal='str', value='array')
    def log_signal(self, timestamp, signal, value):
        import tables
        
        # also check that we didn't already log this instant
        if (signal in self.signal2timestamp) and \
           (self.signal2timestamp[signal] == timestamp):
            return
        self.signal2timestamp[signal] = timestamp

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

            self.debug('Created table %r' % table)
            self.signal2table[signal] = table
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

    def finish(self):        
        tc_close(self.hf)
        if os.path.exists(self.filename):
            os.unlink(self.filename)
        os.rename(self.tmp_filename, self.filename)



