import logging
from logging import INFO, DEBUG, WARN, ERROR, FATAL

import zmq
from zmq.log.handlers import PUBHandler

class EnginePUBHanlder(PUBHandler):
    """A simple PUBHandler subclass that sets root_topic"""
    engine=None
    
    def __init__(self, engine, interface):
        PUBHandler.__init__(self,interface)
        self.engine = engine
        
    @property
    def root_topic(self):
        """this is a property, in case the handler is created
        before the engine gets registered with an id"""
        return "engine.%i"%self.engine.id
    

log = logging.getLogger('ipzmq')
log.setLevel(logging.DEBUG)

