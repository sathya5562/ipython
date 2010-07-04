#!/usr/bin/env python

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from IPython.zmq import messages


loop = ioloop.IOLoop()

ctx = zmq.Context()
notifier = ctx.socket(zmq.XREQ)
notifier.connect('tcp://127.0.0.1:10101')

notifier.send('poke')
print 'recving'
eid, addr = notifier.recv_multipart()
print addr

pair = ctx.socket(zmq.PAIR)
pair.connect(addr)
pair = ZMQStream(pair, loop)

def echo(stream, msg):
    print 'got', msg
    stream.send_multipart(msg)
def unpack_and_print(msg):
    try:
        msg = messages.unpack_message(msg)
    except Exception, e:
        print e
        # pass
    print msg

pair.on_recv(unpack_and_print)

pair.send_multipart([str(eid), "zomg"])
# print pair.recv_multipart()
poller = zmq.Poller()
def poll():
    print poller.poll()
poller.register(pair.socket)
# c = ioloop.Per
# p = ioloop.PeriodicCallback(poll, 1000, loop)
# p.start()
loop.start()