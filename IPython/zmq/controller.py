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
# import messages
# from messages import unpack, send_message_pickle as send_message
from streamsession import Message, default_unpacker as unpack, default_packer as pack
import logging
from log import logger # a Logger object

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
    queue=None
    reg_id=None
    heartbeat=None
    pending=None
    
    def __init__(self, id, uuid, queue, reg_id=None, heartbeat=None):
        logger.info("engine::Engine Connected: %i"%id)
        self.id = id
        self.uuid = uuid
        self.queue = queue
        self.reg_id = reg_id
        self.heartbeat = heartbeat
        self.pending=set()
        
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
    hearts=None
    #
    queues=None
    results=None
    
    def __init__(self, loop, session, context, registrar, client_stream, heartbeat, db_stream, engine_addr=None):
        """
        loop: IOLoop for creating future connections
        session: streamsession for sending serialized data
        context: zmq context (unnecessary?)
        registrar: ZMQStream for engine registration
        client_stream: ZMQStream for client connections
        heartbeat: HeartBeater object for tracking engines
        db_stream: ZMQStream for db connection (NotImplemented)
        engine_addr: zmq address/protocol for engine connections
        """
        self.ids = set()
        self.engines = {}
        self.clients = {}
        self.hearts = {}
        # self.sockets = {}
        self.loop = loop
        self.session = session
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
        self.heartbeat = heartbeat
        if heartbeat is not None:
            # heartbeat.add_new_heart_handler(self.)
            heartbeat.add_heart_failure_handler(self.handle_heart_failure)
            # self.heartbeat.on_recv(self.recv_heartbeat)
        if self.db_stream is not None:
            self.db_stream.on_recv(self.recv_db)
            
        
        self.client_handlers = {'relay_request': self.relay_request,
                                }
        
        # this is the stuff that will move to DB:
        self.queues = {}
        self.results = {}
        self.pending = {}
        
        logger.info("controller::created controller")
    
    def _new_id(self):
        """gemerate a new ID"""
        newid = 0
        while newid in self.ids:
            newid += 1
        return newid, new_uuid()
    
    def _validate_targets(self, targets):
        """turn any valid targets argument into a list of ids"""
        if targets is None:
            targets = self.ids
        if isinstance(targets, int):
            targets = [targets]
        bad_targets = [ t for t in targets if t not in self.ids ]
        if bad_targets:
            raise IndexError("No Such Engine: %s"%bad_targets)
        if not targets:
            raise IndexError("No Engines Registered")
        return targets
    
    def _validate_client_msg(self, msg):
        """validates and unpacks headers of a message. Returns False if invalid,
        (ident, header, parent, content)"""
        ident = msg[0]
        try:
            msg = self.session.unpack_message(msg[1:], content=False)
        except:
            logger.error("client::Invalid Message %s"%msg)
            return False
        
        
        if msg.msg_type == "relay_request":
            if not hasattr(msg.header, 'targets'):
                return False
        elif msg.msg_type == "task_request":
            pass
        
        # session doesn't handle split content for now:
        return ident, msg
        
    
    ###### message handlers and dispatchers ######
    
    def handle_heart_failure(self, heart):
        eid = self.hearts.get(heart, None)
        if eid is None:
            logger.info("heartbeat::ignoring heart failure %s"%heart)
        else:
            self.unregister_engine(eid)
    
    def recv_reg_request(self, msg):
        """"""
        ident = msg[0]
        try:
            msg = self.session.unpack_message(msg[1:])
        except Exception, e:
            logger.error("registration::got bad registration message: %s"%msg)
            raise e
            return
        if msg.msg_type == "registration_request":
            logger.info("registration::registering with "+str(msg))
            try:
                hb = msg.content.heartbeat
            except AttributeError:
                hb=None
            self.register_engine(ident, hb)
        elif msg.msg_type == "unregistration_request":
            try:
                eid=int(ident)
            except:
                logger.error("registration::bad engine id for unregistration: %s"%ident)
                return
            logger.info("registration::unregistering with "+str(msg))
            self.unregister_engine(ident)
        else:
            logger.error("registration::got bad registration message: %s"%msg)
    
    def recv_client_msg(self, msg):
        """"""
        valid = self._validate_client_msg(msg)
        if not valid:
            logger.error("BAD CLIENT MESSAGE: %s"%msg)
            self.session.send(self.client_stream, "failure", ident=msg[0], 
                    content=dict(error=str(SyntaxError("BAD CLIENT MESSAGE"))))
            return
        else:
            ident, msg = valid
        
        # print ident, header, parent, content
        #switch on message type:
        handler = self.client_handlers.get(msg.msg_type, None)
        if handler is None:
            logger.error("BAD MESSAGE TYPE")
            self.session.send(self.client_stream, "failure", ident=ident, 
                    content=dict(error=str(SyntaxError("BAD CLIENT MESSAGE"))))
            return
        else:
            handler(ident, msg)
    
    def relay_request(self, ident, msg):
        try:
            eids = self._validate_targets(msg.header.targets)
            print eids
        except Exception, e:
            self.session.send(self.client_stream, "failure", 
                    content=dict(error=str(e)),ident=ident)
            return
        else:
            relay_ids = []
            for eid in eids:
                
                ec = self.engines[eid]
                omsg = self.session.send(ec.queue, msg.header.submsg_type, content=msg.content, parent=msg.header)
                # print omsg
                self.pending[omsg.msg_id] = Message(dict(client_id=ident, message=omsg))
                ec.pending.add(omsg.msg_id)
                relay_ids.append(omsg.msg_id)
            
            # header = dict(msg_type='relay_success', msg_id=header.msg_id)
            self.session.send(self.client_stream, 'relay_success', 
                content=dict(relay_ids=relay_ids), parent=msg.header)
        
            
    
    
    def recv_db(self, msg):
        """"""
        raise NotImplementedError
    
    def recv_first_engine_msg(self, eid, stream, msg):
        """message handler for the first engine message, to finalize registration"""
        # TODO: finalize registration
        
        # switch over to real engine message handler
        try:
            ident=int(self.session.unpack(msg[0])['username'])
            assert ident==eid
        except Exception, e:            
            logger.error("invalid identity: %s"%msg[0])
            return
        logger.info("registration of engine %i succeeded"%eid)
        stream.on_recv(self.recv_engine_msg)
    
    def _validate_engine_msg(self, msg):
        """validates and unpacks headers of a message. Returns False if invalid,
        (ident, message)"""
        ident = msg[0]
        try:
            eid = int(msg[0])
            assert self.engines.has_key(eid)
        except:
            logger.error("engine::Invalid Engine ID %s"%(ident))
            return False
        
        try:
            msg = self.session.unpack_message(msg[1:], content=False)
        except:
            logger.error("engine.%s::Invalid Message %s"%(ident, msg))
            return False
        
        # session doesn't handle split content for now:
        return eid, msg
    
    def recv_engine_msg(self, msg):
        """"""
        ident = msg[0]
        print "EMSG: ", msg
        
        # msg must have id, header, message at least
        valid = self._validate_engine_msg(msg)
        if not valid:
            logger.error("BAD ENGINE MESSAGE"+ msg)
            return
        else:
            eid, msg = valid
        # print "CONTENT: ",content
        # print self.engines
        ec = self.engines[eid]
        relay = msg.parent_header
        try:
            req = self.pending.pop(relay.msg_id)
            ec.pending.remove(relay.msg_id)
        except KeyError, e:
            logger.error("ERROR: %s"%e)
            logger.error("not a pending message: %s"%relay.msg_id)
            return
        client_id = req.client_id
        # print "PARENT", req.message
        parent = req.message.parent_header
        logger.info("job %s succeeded on engine %i"%(parent.msg_id, int(eid)))
        logger.info("   : relaying result to client %s"%(req.client_id))
        self.session.send(self.client_stream, "relay_result", parent=parent, content=msg.content,ident=client_id)
        
        return
        # 
        # try:
        #     ec.stream.send_multipart(msg[1:])
        # except:
        #     logger.error("ERRORED:%s"%msg)
        #     self.unregister_engine(ec.id)
        
    
    ######   end handlers and dispatchers   ######
    
    def register_engine(self, reg, heart):
        """register a new engine, and create the sockets necessary"""
        eid,uid = self._new_id()
        self.ids.add(eid)
        sock = self.context.socket(zmq.PAIR)
        port = sock.bind_to_random_port(self.engine_addr)
        
        stream = zmqstream.ZMQStream(sock, self.loop)
        stream.on_recv(lambda msg: self.recv_first_engine_msg(eid, stream, msg))
        # stream.on_err(lambda : self.unregister_engine(eid))
        # print "reg", socketkey
        self.engines[eid] = EngineConnector(eid,uid,stream, reg, heart)
        self.hearts[heart] = eid
        self.session.send(self.registrar, "registration_reply", 
                content=dict(addr=self.engine_addr+':'+str(port), id=eid),
                ident=reg)
        # self.registrar.send_multipart([socketkey, str(eid), ])
        return eid,uid
    
    def unregister_engine(self, eid):
        print "UNREGISTERING: ", eid
        self.ids.remove(eid)
        ec = self.engines.pop(eid)
        self.hearts.pop(ec.heartbeat)
        logger.info("registration::unregistering engine: %i"%eid)
        for msg_id in ec.pending:
            req = self.pending.pop(msg_id)
            # HANDLE IT #########################################################
            # client_id = 
    
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
    
        