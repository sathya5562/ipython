#!/usr/bin/env python
import sys
import time
import logging
import uuid
import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from IPython.zmq import messages, log, streamsession as session

ctx = zmq.Context()
loop = ioloop.IOLoop()

thesession = session.StreamSession(username="engine")

class Engine(object):
    """simple version of an engine for demos"""
    id=None
    session=None
    
    def __init__(self, engine_id):
        self.id=engine_id
    
    def _on_registered(self, msg):
        print msg
        
# callbacks:
def echo(stream, msg):
    """log and echo a message"""
    print msg
    log.logger.info('got'+','.join(msg))
    thesession.send(stream, "echo", content=msg)


# config ports, etc.
execfile('config.py', globals())
iface = config['interface']
logport = config['logport']
rport = config['regport']
cport = config['clientport']
hport = config['heartport']

# create/bind log PUB socket
lsock = ctx.socket(zmq.PUB)
connected=False
while not connected:
    try:
        lsock.bind('%s:%i'%(iface,logport))
    except:
        logport += 1
    else:
        connected=True

print "logging on %s:%i"%(iface, logport)
# wait for SUB to connect
# def complete_registration(msg):
time.sleep(1.)
heart = zmq.ThreadsafeDevice(zmq.FORWARDER, zmq.SUB, zmq.XREQ)
heart.setsockopt_in(zmq.SUBSCRIBE, "")
heartid = str(uuid.uuid4())
heart.setsockopt_out(zmq.IDENTITY, heartid)
heart.connect_in("%s:%i"%(iface, hport))
heart.connect_out("%s:%i"%(iface, hport+1))
heart.start()
registrar = ctx.socket(zmq.XREQ)
registrar.connect("%s:%i"%(iface, rport))
thesession.send(registrar, 'registration_request', content=dict(heartbeat=heartid))

print 'msg'
msg = thesession.recv(registrar, 0, content=True)
print msg
id = msg.content['id']
addr = msg.content['addr']
# print header, msg
# eid, addr = notifier.recv_multipart()
engine = Engine(id)
thesession.username=id
# e.id=int(eid)
handler = log.EnginePUBHandler(engine, lsock)
handler.setLevel(logging.DEBUG)
log.logger.addHandler(handler)

pair = ctx.socket(zmq.PAIR)
pair.setsockopt(zmq.IDENTITY, str(id))

pair.connect(str(addr))

pair = ZMQStream(pair, loop)

def unpack_and_print(msg):
    """unpack a message, print and log it"""
    print 'unpack'
    try:
        # print msg
        header = thesession.unpack(msg[0])
        msg = thesession.unpack(msg[-1])
    except Exception, e:
        log.logger.error("ERRORED:"+str(msg))
    # print msg
    log.logger.info(str(header))
    log.logger.info(str(msg))
    # print msg
    thesession.send(pair, "execute_reply", content=dict(stdin=msg['code'],stdout="5",stderr=""), parent=header,ident=str(engine.id))

pair.on_recv(unpack_and_print)
thesession.send(pair, "register_confirmation", parent=msg.header)
        # pair.socket.send_multipart([str(eid), "zomg"])
# print pair.recv_multipart()
# poller = zmq.Poller()
# def poll():
#     print poller.poll()
# poller.register(pair.socket)


loop.start()