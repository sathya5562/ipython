#!/usr/bin/env python


import time
import logging
import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream
from zmq.log import handlers

from IPython.zmq import controller, log, streamsession as session, heartbeat

def setup():
    """setup a basic controller and open client,registrar, and logging ports"""
    ctx = zmq.Context()
    loop = ioloop.IOLoop()
    
    # port config
    execfile('config.py', globals())
    iface = config['interface']
    logport = config['logport']
    rport = config['regport']
    cport = config['clientport']
    hport = config['heartport']
    
    # create/bind log socket
    lsock = ctx.socket(zmq.PUB)
    connected=False
    while not connected:
        try:
            lsock.bind('%s:%i'%(iface,logport))
        except:
            logport = logport + 1
        else:
            connected=True
            
    handler = handlers.PUBHandler(lsock)
    handler.setLevel(logging.DEBUG)
    handler.root_topic = "controller"
    log.logger.addHandler(handler)
    print "logging on %s:%i"%(iface, logport)
    # wait for logger SUB
    time.sleep(1.)
    
    # Engine registrar socket
    reg = ZMQStream(ctx.socket(zmq.XREP), loop)
    reg.bind("%s:%i"%(iface, rport))
    
    # client socket
    c = ZMQStream(ctx.socket(zmq.XREP), loop)
    c.bind("%s:%i"%(iface, cport))
    # port += 1
    thesession = session.StreamSession(username="controller")
    # create controller
    hpub = ctx.socket(zmq.PUB)
    hpub.bind("%s:%i"%(iface, hport))
    hrep = ctx.socket(zmq.XREP)
    hrep.bind("%s:%i"%(iface, hport+1))
    
    hb = heartbeat.HeartBeater(loop, ZMQStream(hpub,loop), ZMQStream(hrep,loop), 5000)
    
    con = controller.Controller(loop, thesession, ctx, reg, c, hb, None)
    
    return loop
    

if __name__ == '__main__':
    loop = setup()
    loop.start()