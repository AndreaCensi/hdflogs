PROCGRAPH_LOG_GROUP = 'procgraph'


import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


from .hdflogwriter import *
from .tables_cache import *
from .hdflogreader import *
