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
    # config={}
    execfile('config.py', globals())
    iface = config['interface']
    logport = config['logport']
    rport = config['regport']
    cport = config['clientport']
    cqport = config['cqueueport']
    eqport = config['equeueport']
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
    time.sleep(.5)
    
    # Engine registrar socket
    reg = ZMQStream(ctx.socket(zmq.XREP), loop)
    reg.bind("%s:%i"%(iface, rport))
    
    # clientele socket
    c = ZMQStream(ctx.socket(zmq.XREP), loop)
    c.bind("%s:%i"%(iface, cport))
    # port += 1
    thesession = session.StreamSession(username="controller")
    
    # heartbeat
    hpub = ctx.socket(zmq.PUB)
    hpub.bind("%s:%i"%(iface, hport))
    hrep = ctx.socket(zmq.XREP)
    hrep.bind("%s:%i"%(iface, hport+1))
    
    hb = heartbeat.HeartBeater(loop, ZMQStream(hpub,loop), ZMQStream(hrep,loop), 5000)
    
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, "")
    
    monport = sub.bind_to_random_port(iface)
    sub = ZMQStream(sub, loop)
    
    q = zmq.TSMonitoredQueue(zmq.XREP, zmq.XREP, zmq.PUB)
    q.bind_in("%s:%i"%(iface, cqport))
    q.bind_out("%s:%i"%(iface, eqport))
    q.connect_mon("%s:%i"%(iface, monport))
    q.start()
    time.sleep(.25)
    con = controller.Controller(loop, thesession, reg, c, sub, hb, None, 
            "%s:%s"%(iface, eqport),"%s:%s"%(iface, hport), None)
    
    return loop
    

if __name__ == '__main__':
    loop = setup()
    loop.start()