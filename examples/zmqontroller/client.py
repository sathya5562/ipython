#!/usr/bin/env python

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream
from IPython.zmq import streamsession as session
from IPython.zmq.messages import send_message_pickle as send_message
import uuid

thesession = session.StreamSession()

def poit():
    print "POIT"

def unpack_and_print(msg):
    try:
        msg = thesession.unpack(msg[-1])
    except Exception, e:
        print e
        # pass
    print msg

ctx = zmq.Context()

loop = ioloop.IOLoop()
sock = ctx.socket(zmq.XREQ)
client = ZMQStream(sock, loop)
client.on_recv(unpack_and_print)
client.on_send(poit)

sock.connect('tcp://127.0.0.1:10102')
sock.setsockopt(zmq.IDENTITY, thesession.username)
# stream = ZMQStream()
# header = dict(msg_id = uuid.uuid4().bytes, msg_type='relay', id=0)
parent = dict(targets=2)
# content = "GARBAGE"
thesession.send(client, "relay_request", subheader=dict(targets=None, submsg_type="execute_request"), content=dict(code="print a"))
# send_message(client, (header, content))
# print thesession.recv(client, 0)

loop.start()
