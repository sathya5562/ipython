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
from streamsession import Message, wrap_exception # default_unpacker as unpack, default_packer as pack
import logging
from log import logger # a Logger object

# from messages import json # use the same import switches

#-----------------------------------------------------------------------------
# Code
#-----------------------------------------------------------------------------

class EngineConnector(object):
    """A simple object for accessing the various zmq connections of an object.
    Attributes are:
    id (int): engine ID
    uuid (str): uuid (unused?)
    queue (str): identity of queue's XREQ socket
    registration (str): identity of registration XREQ socket
    heartbeat (str): identity of heartbeat XREQ socket
    """
    id=0
    uuid=None
    queue=None
    registration=None
    heartbeat=None
    pending=None
    
    def __init__(self, id, uuid, queue, registration, heartbeat=None):
        logger.info("engine::Engine Connected: %i"%id)
        self.id = id
        self.uuid = uuid
        self.queue = queue
        self.registration = registration
        self.heartbeat = heartbeat
        
class Controller(object):
    """The IPython Controller with 0MQ connections
    
    Parameters
    ==========
    loop: zmq IOLoop instance
    session: StreamSession object
    <removed> context: zmq context for creating new connections (?)
    registrar: ZMQStream for engine registration requests (XREP)
    clientele: ZMQStream for client connections (XREP)
                not used for jobs, only query/control commands
    queue: ZMQStream for monitoring the command queue (SUB)
    heartbeat: HeartBeater object checking the pulse of the engines
    db_stream: connection to db for out of memory logging of commands
                NotImplemented
    queue_addr: zmq connection address of the XREP socket for the queue
    hb_addr: zmq connection address of the PUB socket for heartbeats
    task_addr: zmq connection address of the XREQ socket for task queue
    """
    
    ids=None
    engines=None
    clients=None
    hearts=None
    pending=None
    results=None
    
    loop=None
    registrar=None
    clientelle=None
    queue=None
    heartbeat=None
    db=None
    queue_addr=None
    hb_addr=None
    task_addr=None
    
    
    def __init__(self, loop, session, registrar, clientele, queue, heartbeat, db, queue_addr, hb_addr, task_addr=None):
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
        self.by_ident = {}
        self.clients = {}
        self.hearts = {}
        
        # self.sockets = {}
        self.loop = loop
        self.session = session
        self.registrar = registrar
        self.clientele = clientele
        self.queue = queue
        self.heartbeat = heartbeat
        self.db = db
        
        self.queue_addr = queue_addr
        self.hb_addr = hb_addr
        self.task_addr = task_addr
        
        self.addrs = dict(queue=queue_addr, heartbeat=hb_addr, task=task_addr)
        
        # register our callbacks
        self.registrar.on_recv(self.dispatch_register_request)
        self.clientele.on_recv(self.dispatch_client_msg)
        self.queue.on_recv(self.dispatch_queue_traffic)
        
        self.heartbeat = heartbeat
        if heartbeat is not None:
            heartbeat.add_heart_failure_handler(self.handle_heart_failure)
        
        if self.db is not None:
            self.db.on_recv(self.dispatch_db)
            
        self.client_handlers = {'queue_status': self.queue_status,
                                }
        # 
        # this is the stuff that will move to DB:
        self.results = {} # completed results
        self.pending = {} # pending messages, keyed by msg_id
        self.queues = {} # pending msg_ids keyed by engine_id
        
        logger.info("controller::created controller")
    
    def _new_id(self):
        """gemerate a new ID"""
        newid = 0
        while newid in self.ids:
            newid += 1
        return newid, new_uuid()
    
    
    ######### message validation methods #############
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
        client_id = msg[0]
        try:
            msg = self.session.unpack_message(msg[1:], content=True)
        except:
            logger.error("client::Invalid Message %s"%msg)
            return False
        
        msg_type = msg.get('msg_type', None)
        if msg_type is None:
            return False
        header = msg.get('header')
        # session doesn't handle split content for now:
        return client_id, msg
        
    
    
    ###### message handlers and dispatchers ######
    
    def dispatch_register_request(self, msg):
        """"""
        logger.debug("registration::dispatch_register_request(%s)"%msg)
        reg_id = msg[0]
        try:
            msg = self.session.unpack_message(msg[1:])
        except Exception, e:
            logger.error("registration::got bad registration message: %s"%msg)
            raise e
            return
        msg_type = msg['msg_type']
        content = msg['content']
        if msg_type == "registration_request":
            try:
                queue = content['queue']
            except KeyError:
                logger.error("registration::queue not specified")
                return
            hb = content.get('heartbeat', None)
            logger.info("registration::registering with "+str(msg))
            self.register_engine(queue, reg_id, hb)
        elif msg_type == "unregistration_request":
            try:
                eid = content['id']
            except:
                logger.error("registration::bad engine id for unregistration: %s"%reg_id)
                return
            logger.info("registration::unregistering with "+str(msg))
            self.unregister_engine(eid)
        else:
            logger.error("registration::got bad registration message: %s"%msg)
    
    def dispatch_queue_traffic(self, msg):
        """all queue messages will have both """
        logger.debug("queue traffic: %s"%msg)
        if msg[0] == 'in':
            self.save_queue_request(msg[1:])
        elif msg[0] == 'out':
            self.save_queue_result(msg[1:])
        else:
            logger.error("Invalid message tag: %s"%msg[0])
        
    
    def dispatch_client_msg(self, msg):
        """"""
        logger.debug("client::%s"%msg)
        ident = msg[0]
        valid = self._validate_client_msg(msg)
        try:
            assert valid, "Bad Client Message: %s"%msg
        except:
            content = wrap_exception()
            logger.error("Bad Client Message: %s"%msg)
            self.session.send(self.client_stream, "controller_error", ident=ident, 
                    content=content)
            return
        else:
            ident, msg = valid
        
        # print ident, header, parent, content
        #switch on message type:
        handler = self.client_handlers.get(msg.msg_type, None)
        try:
            assert handler is not None, "Bad Message Type: %s"%msg.msg_type
        except:
            content = wrap_exception()
            logger.error("Bad Message Type: %s"%msg.msg_type)
            self.session.send(self.client_stream, "controller_error", ident=ident, 
                    content=content)
            return
        else:
            handler(ident, msg)
            
    def dispatch_db(self, msg):
        """"""
        raise NotImplementedError
    
    ######   end dispatchers   ######
    ######   begin handlers   ######
    
    def handle_heart_failure(self, heart):
        """handler to attach to heartbeater.
        called when a previously registeredheart fails to respond to beat request.
        triggers unregistration"""
        logger.debug("heartbeat::handle_heart_failure(%s)"%heart)
        eid = self.hearts.get(heart, None)
        if eid is None:
            logger.info("heartbeat::ignoring heart failure %s"%heart)
        else:
            self.unregister_engine(eid)
    
    def save_queue_request(self, msg):
        client_id, queue_id = msg[:2]
        try:
            msg = self.session.unpack(msg[2:],content=False)
        except:
            logger.error("queue::client %s sent invalid message to %s: %s"%(client_id, queue_id, msg[2:]))
        
        eid = self.by_ident.get(queue_id, None)
        if eid is None:
            logger.error("queue::target %s not registered"%queue_id)
            logger.debug("queue::    valid are: %s"%(self.by_ident.keys()))
            return
            
        header = msg['header']
        msg_id = header['msg_id']
        self.pending[msg_id] = msg
        self.queues[eid].add(msg_id)
    
    def save_queue_result(self, msg):
        queue_id, client_id = msg[:2]
        try:
            msg = self.session.unpack(msg[2:],content=False)
        except:
            logger.error("queue::engine %s sent invalid message to %s: %s"%(client_id, queue_id, msg[2:]))
        
        eid = self.by_ident.get(queue_id, None)
        if eid is None:
            logger.error("queue::unknown engine %s is sending a reply: "%queue_id)
            logger.debug("queue::       %s"%msg[2:])
            return
        
        parent = msg['parent_header']
        msg_id = parent['msg_id']
        self.results[msg_id] = msg
        if msg_id in self.pending:
            self.pending.pop(msg_id)
            self.queus[eid].remove(msg_id)
            
        
    
    def register_engine(self, queue, reg, heart):
        """register a new engine, and create the socket(s) necessary"""
        eid,uid = self._new_id()
        logger.debug("registration::register_engine(%i, %s,%s, %s)"%(eid, queue, reg, heart))
        self.ids.add(eid)
        
        self.engines[eid] = EngineConnector(eid, uid, queue, reg, heart)
        self.by_ident[queue] = self.engines[eid]
        self.queues[eid] = set()
        
        self.hearts[heart] = eid
        content = dict(id=eid)
        content.update(self.addrs)
        self.session.send(self.registrar, "registration_reply", 
                content=content, 
                ident=reg)
        return eid,uid
    
    def unregister_engine(self, eid):
        logger.info("registration::unregister_engine(%s)"%eid)
        self.ids.remove(eid)
        ec = self.engines.pop(eid)
        self.hearts.pop(ec.heartbeat)
        self.by_ident.pop(ec.queue)
        
        for msg_id in self.queues.pop(eid):
            msg = self.pending.pop(msg_id)
            # HANDLE IT #########################################################
            
    
    def check_load(self, targets=None):
        targets = self._build_targets(targets)
        return [1]*len(self.ids)
        for eid in targets:
            ec = self.engines[eid]
    
    def queue_status(self, msg):
        pass


############ OLD METHODS for Python Relay Controller ###################
    def _validate_engine_msg(self, msg):
        """validates and unpacks headers of a message. Returns False if invalid,
        (ident, message)"""
        ident = msg[0]
        try:
            msg = self.session.unpack_message(msg[1:], content=False)
        except:
            logger.error("engine.%s::Invalid Message %s"%(ident, msg))
            return False
        
        try:
            eid = msg.header.username
            assert self.engines.has_key(eid)
        except:
            logger.error("engine::Invalid Engine ID %s"%(ident))
            return False
        
        return eid, msg
    def relay_request(self, ident, msg):
        """relay a message from a client to one or more engines
        """
        try:
            eids = self._validate_targets(msg.header.targets)
        except:
            content = wrap_exception()
            self.session.send(self.client_stream, "controller_error", 
                    content=content,ident=ident)
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
        
    def dispatch_first_engine_msg(self, eid, stream, msg):
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
        stream.on_recv(self.dispatch_engine_msg)
    
    def dispatch_engine_msg(self, msg):
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
    

        