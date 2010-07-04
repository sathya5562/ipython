
import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from IPython.zmq import controller


def setup():
    """docstring for main"""
    iface = '127.0.0.1'
    port = 10101
    
    ctx = zmq.Context()
    loop = ioloop.IOLoop()
    reg = ZMQStream(ctx.socket(zmq.XREP), loop)
    reg.bind("tcp://%s:%i"%(iface, port))
    port += 1
    
    c = ZMQStream(ctx.socket(zmq.XREP), loop)
    c.bind("tcp://%s:%i"%(iface, port))
    port += 1
    
    con = controller.Controller(loop, ctx, reg, c, None, None)
    
    return loop
    

if __name__ == '__main__':
    loop = setup()
    loop.start()