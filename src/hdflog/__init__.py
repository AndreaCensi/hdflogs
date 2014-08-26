PROCGRAPH_LOG_GROUP = 'procgraph'


import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class HDFLogOptions():
    allow_delta0 = True
    

from .hdflogwriter import *
from .tables_cache import *
from .hdflogreader import *
