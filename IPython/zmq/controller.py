#!/usr/bin/env python
# encoding: utf-8

"""The IPython Controller with 0MQ
This is the master object that handles connections from engines, clients, and 
"""
#-----------------------------------------------------------------------------
#  Copyright (C) 2008-2009  The IPython Development Team
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING, distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import zmq
from zmq.eventloop import zmqstream
from uuid import uuid4 as new_uuid
import messages
from messages import unpack, send_message_pickle as send_message
from session import Message
import logging

# from messages import json # use the same import switches

#-----------------------------------------------------------------------------
# Code
#-----------------------------------------------------------------------------

class EngineConnector(object):
    """A simple object for accessing the various zmq connections of an object.
    Attributes are:
    heartbeat
    queue
    """
    queue = None
    id=0
    uuid=None
    stream=None
    def __init__(self, id, uuid, stream):
        self.id = id
        self.uuid = uuid
        self.stream = stream
        
class Controller(object):
    """The IPython Controller with 0MQ connections"""
    
    ids=None
    engines=None
    clients=None
    loop=None
    context=None
    registrar=None
    client_stream=None
    heartbeat=None
    db_stream=None
    engine_addr='tcp://127.0.0.1'
    #
    queues=None
    results=None
    
    def __init__(self, loop, context, registrar, client_stream, heartbeat, db_stream, engine_addr=None):
        """"""
        self.ids = set()
        self.engines = {}
        self.clients = {}
        # self.sockets = {}
        self.loop = loop
        self.context = context
        self.registrar = registrar
        self.client_stream = client_stream
        self.heartbeat = heartbeat
        self.db_stream = db_stream
        if engine_addr:
            self.engine_addr = engine_addr
        # register our callbacks
        self.registrar.on_recv(self.recv_reg_request)
        self.client_stream.on_recv(self.recv_client_msg)
        if heartbeat is not None:
            self.heartbeat.on_recv(self.recv_heartbeat)
        if self.db_stream is not None:
            self.db_stream.on_recv(self.recv_db)
            
        

        # this is the stuff that will move to DB:
        self.queues = {}
        self.results = {}
    
    def _new_id(self):
        """gemerate a new ID"""
        newid = 0
        while newid in self.ids:
            newid += 1
        return newid, new_uuid()
    
    def _build_targets(self, targets):
        """turn any valid targets argument into a list of ids"""
        if targets is None:
            targets = self.ids
        if isinstance(targets, int):
            targets = [targets]
        return targets
    
    def _validate_client_msg(self, msg):
        if len(msg) < 3:
            return False
        cid, header = msg[:2]
        content = msg[2:]
        
        try:
            header = unpack(header)
        except:
            print "couldn't unpack header"
            return False
        
        if not isinstance(header, dict):
            print "header not a dict, but a ", type(dict)
            return False
        for key in 'msg_id msg_type'.split():
            if not header.has_key(key):
                return False
        msg = Message(header)
        
        if msg.msg_type not in messages.MSG_TYPES:
            return False
        if msg.msg_type == 'relay':
            if not header.has_key('id'):
                return False
        
        return cid, msg, content
        
        
    ###### message handlers and dispatchers ######
    
    def recv_reg_request(self, msg):
        """"""
        print "reg", msg
        
        self.register_engine(msg[0])
    
    def recv_client_msg(self, msg):
        """"""
        valid = self._validate_client_msg(msg)
        if not valid:
            print "BAD CLIENT MESSAGE", msg
            header = dict(msg_type='failure', msg_id = uuid.uuid4().bytes)
            
            send_message(self.client_stream, [msg[0], header, SyntaxError("BAD CLIENT MESSAGE")])
            
            return
        else:
            cid, header, content = valid
        
        #switch on message type:
        if header.msg_type == 'relay':
            eid = header.id
            if eid not in self.ids:
                header = dict(msg_type='failure', msg_id=header.msg_id)
                send_message(self.client_stream, [cid, header, KeyError('no such engine')])
                return
            else:
                ec = self.engines[eid]
                print "relaying"
                ec.stream.send_multipart(content)
                header = dict(msg_type='relay_success', msg_id=header.msg_id)
                send_message(self.client_stream, [cid, header, "YAY"])
        else:
            raise Exception("BAD MESSAGE TYPE")
        
            
        
    
    def recv_heartbeat(self, msg):
        """"""
    
    def recv_db(self, msg):
        """"""
    
    def recv_engine_msg(self, msg):
        """"""
        print "EMSG: ", msg
        
        # currently just echo
        try:
            ec = self.engines[int(msg[0])]
        except:
            print "no such engine: %s"%msg[:1]
            return
        try:
            ec.stream.send_multipart(msg[1:])
        except:
            print "ERRORED:", msg
            self.unregister_engine(ec.id)
        
    
    ######   end   handlers and dispatchers ######
    
    def register_engine(self, socketkey):
        """register a new engine, and create the sockets necessary"""
        eid,uid = self._new_id()
        self.ids.add(eid)
        sock = self.context.socket(zmq.PAIR)
        port = sock.bind_to_random_port(self.engine_addr)
        
        stream = zmqstream.ZMQStream(sock, self.loop)
        stream.on_recv(self.recv_engine_msg)
        # stream.on_err(lambda : self.unregister_engine(eid))
        
        self.engines[eid] = EngineConnector(eid,uid,stream)
        
        self.registrar.send_multipart([socketkey, str(eid), self.engine_addr+':'+str(port)])
        return eid,uid
    
    def unregister_engine(self, eid):
        print "UNREGISTERING: ", eid
        self.ids.remove(eid)
        self.engines.pop(eid)
    
    def check_load(self, targets=None):
        targets = self._build_targets(targets)
        return [1]*len(self.ids)
        for eid in targets:
            ec = self.engines[eid]
            # self.heartbeat_stream.send()
    
    def handle_error(self,socket):
        """handle a POLLERR event"""
        # ???
    
    def handle_receive(self, socket):
        """handle a message waiting in a queue"""
    
        