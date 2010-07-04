#!/usr/bin/env python
import time
import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream
from IPython.zmq import messages
from IPython.zmq.messages import send_message_pickle as send_message
import uuid
def poit():
    print "POIT"

def unpack_and_print(msg):
    try:
        msg = messages.unpack_message(msg)
    except Exception, e:
        print e
        # pass
    print msg

ctx = zmq.Context()

loop = ioloop.IOLoop()
sock = ctx.socket(zmq.XREQ)
client = ZMQStream(sock, loop)
client.on_recv(unpack_and_print)
# client.on_send(poit)

sock.connect('tcp://127.0.0.1:10102')

header = dict(msg_id = uuid.uuid4().bytes, msg_type='relay', id=0)
content = "GARBAGE"
def dosend():
    # print 'a'
    send_message(client, (header, content))
# for i in range(1000):
    
    # break
    # time.sleep(1)
pc = ioloop.PeriodicCallback(dosend, 10, loop)
pc.start()
loop.start()
