#!/usr/bin/env python
"""A semi-synchronous Client for the ZMQ controller"""

import time
import threading

from functools import wraps

try:
    from decorator import decorator
except:
    decorator = lambda f: f

import streamsession as ss
import zmq

def _push(ns):
    globals().update(ns)

def _pull(keys):
    g = globals()
    return map(g.get, keys)

# decorators for methods:
@decorator
def spinfirst(f):
    @wraps(f)
    def spun_method(self, *args, **kwargs):
        self.spin()
        return f(self, *args, **kwargs)
    return spun_method

@decorator
def checktargets(f):
    @wraps(f)
    def checked_method(self, *args, **kwargs):
        self._build_targets(kwargs['targets'])
        return f(self, *args, **kwargs)
    return checked_method


# class _ZMQEventLoopThread(threading.Thread):
#     
#     def __init__(self, loop):
#         self.loop = loop
#         threading.Thread.__init__(self)
#     
#     def run(self):
#         self.loop.start()
# 
class Client(object):
    """A semi-synchronous client to the IPython ZMQ controller
    
    Attributes
    ----------
    ids : set
        a set of engine IDs
        requesting the ids attribute always synchronizes
        the registration state. To request ids without synchronization,
        use _ids
    
    history : list of msg_ids
        a list of msg_ids, keeping track of all the execution
        messages you have submitted
    
    outstanding : set of msg_ids
        a set of msg_ids that have been submitted, but whose
        results have not been received
    
    results : dict
        a dict of all our results, keyed by msg_id
    
    block : bool
        determines default behavior when block not specified
        in execution methods
    
    Methods
    -------
    spin : flushes incoming results and registration state changes
            control methods spin, and requesting `ids` also ensures up to date
            
    barrier : wait on one or more msg_ids
    
    execution methods: apply/apply_bound/apply_to
        legacy: execute, run
    
    control methods: queue_status, get_result
    
    """
    
    
    _connected=False
    _engines=None
    registration_socket=None
    controller_socket=None
    notification_socket=None
    queue_socket=None
    task_socket=None
    block = False
    outstanding=None
    results = None
    history = None
    
    def __init__(self, addr, context=None, username=None):
        if context is None:
            context = zmq.Context()
        self.context = context
        self.addr = addr
        if username is None:
            self.session = ss.StreamSession()
        else:
            self.session = ss.StreamSession(username)
        self.registration_socket = self.context.socket(zmq.PAIR)
        self.registration_socket.setsockopt(zmq.IDENTITY, self.session.session)
        self.registration_socket.connect(addr)
        self._engines = {}
        self._ids = set()
        self.outstanding=set()
        self.results = {}
        self.history = []
        self._connect()
        
        self._notification_handlers = {'registration_notification' : self._register_engine,
                                    'unregistration_notification' : self._unregister_engine,
                                    }
        self._queue_handlers = {'execute_reply' : self._handle_execute_reply,
                                'apply_reply' : self._handle_apply_reply}
        
    
    @property
    def ids(self):
        self._flush_notifications()
        return self._ids
    
    def _update_engines(self, engines):
        for k,v in engines.iteritems():
            eid = int(k)
            self._engines[eid] = v
            self._ids.add(eid)
    
    def _build_targets(self, targets):
        if targets is None or targets == 'all':
            targets = self._ids
        elif isinstance(targets, int):
            targets = [targets]
        return [self._engines[t] for t in targets], list(targets)
    
    def _connect(self):
        """setup all our socket connections to the controller"""
        if self._connected:
            return
        self._connected=True
        self.session.send(self.registration_socket, 'connection_request')
        msg = self.session.recv(self.registration_socket,mode=0)[-1]
        msg = ss.Message(msg)
        content = msg.content
        if content.status == 'ok':
            if content.queue:
                self.queue_socket = self.context.socket(zmq.PAIR)
                self.queue_socket.setsockopt(zmq.IDENTITY, self.session.session)
                self.queue_socket.connect(content.queue)
            if content.task:
                self.task_socket = self.context.socket(zmq.PAIR)
                self.task_socket.setsockopt(zmq.IDENTITY, self.session.session)
                self.task_socket.connect(content.task)
            if content.notification:
                self.notification_socket = self.context.socket(zmq.SUB)
                self.notification_socket.connect(content.notification)
                self.notification_socket.setsockopt(zmq.SUBSCRIBE, "")
            if content.controller:
                self.controller_socket = self.context.socket(zmq.PAIR)
                self.controller_socket.setsockopt(zmq.IDENTITY, self.session.session)
                self.controller_socket.connect(content.controller)
            self._update_engines(dict(content.engines))
                
        else:
            self._connected = False
            raise Exception("Failed to connect!")
    
    #### handlers and callbacks for incoming messages #######
    def _register_engine(self, msg):
        content = msg['content']
        eid = content['id']
        d = {eid : content['queue']}
        self._update_engines(d)
        self._ids.add(int(eid))

    def _unregister_engine(self, msg):
        # print 'unregister',msg
        content = msg['content']
        eid = int(content['id'])
        if eid in self._ids:
            self._ids.remove(eid)
            self._engines.pop(eid)
        
    def _handle_execute_reply(self, msg):
        # msg_id = msg['msg_id']
        parent = msg['parent_header']
        msg_id = parent['msg_id']
        if msg_id not in self.outstanding:
            print "got unknown result: %s"%msg_id
        else:
            self.outstanding.remove(msg_id)
        self.results[msg_id] = msg['content']
    
    def _handle_apply_reply(self, msg):
        # print msg
        # msg_id = msg['msg_id']
        parent = msg['parent_header']
        msg_id = parent['msg_id']
        if msg_id not in self.outstanding:
            print "got unknown result: %s"%msg_id
        else:
            self.outstanding.remove(msg_id)
        content = msg['content']
        if content['status'] == 'ok':
            self.results[msg_id] = ss.unserialize_object(msg['buffers'])
        else:
            self.results[msg_id] = content
    
    def _flush_notifications(self):
        "flush incoming notifications of engine registrations"
        msg = self.session.recv(self.notification_socket, mode=zmq.NOBLOCK)
        while msg is not None:
            msg = msg[-1]
            msg_type = msg['msg_type']
            handler = self._notification_handlers.get(msg_type, None)
            if handler is None:
                raise Exception("Unhandled message type: %s"%msg.msg_type)
            else:
                handler(msg)
            msg = self.session.recv(self.notification_socket, mode=zmq.NOBLOCK)
    
    def _flush_results(self, sock):
        "flush incoming task or queue results"
        msg = self.session.recv(sock, mode=zmq.NOBLOCK)
        while msg is not None:
            msg = msg[-1]
            msg_type = msg['msg_type']
            handler = self._queue_handlers.get(msg_type, None)
            if handler is None:
                raise Exception("Unhandled message type: %s"%msg.msg_type)
            else:
                handler(msg)
            msg = self.session.recv(sock, mode=zmq.NOBLOCK)
    
    ############ begin real methods #############
    
    def spin(self):
        """flush incoming notifications and execution results."""
        if self.notification_socket:
            self._flush_notifications()
        if self.queue_socket:
            self._flush_results(self.queue_socket)
        if self.task_socket:
            self._flush_results(self.task_socket)
    
    @spinfirst
    def queue_status(self, targets=None, verbose=False):
        """fetch the status of engine queues
        
        Parameters
        ----------
        targets : int/str/list of ints/strs
                the engines on which to execute
                default : all
        verbose : bool
                whether to return 
                
        """
        targets = self._build_targets(targets)[1]
        content = dict(targets=targets)
        self.session.send(self.controller_socket, "queue_status", content=content)
        idents,msg = self.session.recv(self.controller_socket, 0)
        return msg['content']
        
    @spinfirst
    def clear(self, targets=None):
        pass
    
    @spinfirst
    def abort(self, targets=None):
        pass
    
    # @spinfirst
    def execute(self, code, targets=None, block=None):
        """executes `code` on `targets` in blocking or nonblocking manner.
        
        Parameters
        ----------
        code : str
                the code string to be executed
        targets : int/str/list of ints/strs
                the engines on which to execute
                default : all
        block : bool
                whether or not to wait until done
        """
        queues,targets = self._build_targets(targets)
        
        block = self.block if block is None else block
        msg_ids = []
        content = dict(code=code)
        for q in queues:
            msg = self.session.send(self.queue_socket, 'execute_request', 
                    content=content, ident=q)
            msg_id = msg['msg_id']
            msg_ids.append(msg_id)
            self.outstanding.add(msg_id)
            self.history.append(msg_id)
        if block:
            self.barrier(msg_ids)
            result = {}
            for target,mid in zip(targets, msg_ids):
                    result[target] = self.results[mid]

            return result
        else:
            return msg_ids
        
    def run(self, code, block=None):
        """runs code on an engine"""
        a = time.time()
        block = self.block if block is None else block
        content = dict(code=code)
        b = time.time()
        msg = self.session.send(self.task_socket, 'execute_request', 
                content=content)
        c = time.time()
        msg_id = msg['msg_id']
        self.outstanding.add(msg_id)
        self.history.append(msg_id)
        d = time.time()
        if block:
            self.barrier(msg_id)
            return self.results[msg_id]
        else:
            return msg_id
    
    def _apply(self, bound, f, *args, **kwargs):
        """the underlying method called by apply() and apply_bound()"""
        bufs = ss.pack_apply_message(f,args,kwargs)
        content = dict(bound=bound)
        msg = self.session.send(self.task_socket, "apply_request", 
                content=content, buffers=bufs)
        msg_id = msg['msg_id']
        self.outstanding.add(msg_id)
        self.history.append(msg_id)
        if self.block:
            self.barrier(msg_id)
            return self.results[msg_id]
        else:
            return msg_id
    
    def apply(self, f, *args, **kwargs):
        """calls f(*args, **kwargs) on a remote engine, returning the result.
        
        This method does not involve the engine's namespace.
        
        if self.block is False:
            returns msg_id
        else:
            returns actual result of f(*args, **kwargs)
        """
        return self._apply(False, f, *args, **kwargs)
    
    def apply_bound(self, f, *args, **kwargs):
        """calls f(*args, **kwargs) on a remote engine. This does get
        executed in an engine's namespace. The controller selects the 
        target engine via 0MQ XREQ load balancing.
        
        if self.block is False:
            returns msg_id
        else:
            returns actual result of f(*args, **kwargs)
        """
        return self._apply(True, f, *args, **kwargs)
    
    def apply_to(self, targets, f, *args, **kwargs):
        """calls f(*args, **kwargs) on a specific engine.
        
        if self.block is False:
            returns msg_id
        else:
            returns actual result of f(*args, **kwargs)
        """
        queues,targets = self._build_targets(targets)
        assert len(queues) == 1
        queue = queues[0]
        bufs = ss.pack_apply_message(f,args,kwargs)
        content = dict(bound=True)
        msg = self.session.send(self.queue_socket, "apply_request", 
                content=content, buffers=bufs,ident=queue)
        msg_id = msg['msg_id']
        self.outstanding.add(msg_id)
        self.history.append(msg_id)
        if self.block:
            self.barrier(msg_id)
            return self.results[msg_id]
        else:
            return msg_id
    
    def push(self, target, ns, block=None):
        """push the contents of `ns` into the namespace on `target`"""
        block = self.block if block is None else block
        saveblock = self.block
        self.block = block
        result = self.apply_to(target, _push, ns)
        self.block = saveblock
        return result
    
    @spinfirst
    def pull(self, target, keys, block=True):
        """pull objects from `target`'s namespace by `keys`"""
        block = self.block if block is None else block
        saveblock = self.block
        self.block = block
        result = self.apply_to(target, _pull, keys)
        self.block = saveblock
        return result
    
    def barrier(self, msg_ids=None, timeout=-1):
        """waits on one or more `msg_ids`, for up to `timeout` seconds.
        
        Parameters
        ----------
        msg_ids : int, str, or list of ints and/or strs
                ints are indices to self.history
                strs are msg_ids
                default: wait on all outstanding messages
        timeout : float
                a time in seconds, after which to give up.
                default is -1, which means no timeout
        
        Returns
        -------
        True : when all msg_ids are done
        False : timeout reached, msg_ids still outstanding
        """
        tic = time.time()
        if msg_ids is None:
            theids = self.outstanding
        else:
            if isinstance(msg_ids, (int, str)):
                msg_ids = [msg_ids]
            theids = set()
            for msg_id in msg_ids:
                if isinstance(msg_id, int):
                    msg_id = self.history[msg_id]
                theids.add(msg_id)
        while theids.intersection(self.outstanding):
            self.spin()
            if timeout >= 0 and ( time.time()-tic ) > timeout:
                break
            time.sleep(0)
        return len(theids.intersection(self.outstanding)) == 0
    
    @spinfirst
    def get_result(self, msg_id):
        """returns the result of the execute or task request with `msg_id`"""
        if isinstance(msg_id, int):
            msg_id = self.history[msg_id]
        
        content = dict(msg_id=msg_id)
        msg = self.session.send(self.controller_socket, "result_request", content=content)
        idents,msg = self.session.recv(self.controller_socket, 0)
        return msg
    
    