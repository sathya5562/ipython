=====================
Message Specification
=====================

Note: this is adapted from the pyzmq kernel example.

Note: not all of these have yet been fully fleshed out, but the key ones are,
see kernel and frontend files for actual implementation details.

General Message Format
=====================

General message format::

    {
        header : { 'msg_id' : uuid, # output of str(uuid.uuid4())
            'msg_type' : 'string_message_type',
            'username' : 'name',
            'session' : uuid,
            ... extra keys can be specified via a subheader dict ...
           },
        parent_header : dict,
        msg_type : 'string_message_type',
        content : blackbox_dict , # Must be a dict
    }

Messages are sent by StreamSessions in packed parts, as follows::

    [ ident, pheader, pparent, pcontent ]

where ident is the zmq socket identifier, and each following object is a 
packed version (json or pickle or custom) of the components of the message

sending with a stream session:
StreamSession.send(socket (or ZMQStream), msg_type, content)

Side effect: (PUB/SUB)
======================

# msg_type = 'stream'
content = {
    name : 'stdout',
    data : 'blob',
}

# msg_type = 'pyin'
content = {
    code = 'x=1',
}

# msg_type = 'pyout'
content = {
    data = 'repr(obj)',
    prompt_number = 10
}

# msg_type = 'pyerr'
content = {
    traceback : 'full traceback',
    exc_type : 'TypeError',
    exc_value :  'msg'
}

# msg_type = 'file'
content = {
    path = 'cool.jpg',
    data : 'blob'
}

Request/Reply
=============

Execute
-------

Request:

# msg_type = 'execute_request'
content = {
    code : 'a = 10',
}

Reply:

# msg_type = 'execute_reply'
content = {
  'status' : 'ok' OR 'error' OR 'abort'
  # data depends on status value
}

Complete
--------

# msg_type = 'complete_request'
content = {
    text : 'a.f',    # complete on this
    line : 'print a.f'    # full line
}

# msg_type = 'complete_reply'
content = {
    matches : ['a.foo', 'a.bar']
}

Controller
----------

# for unhandled messages:
# msg_type = 'controller_error'
content = {
    status : 'error',
    'traceback' : str,
    'etype' : str,
    'evalue' : str
}

# msg_type = 'registration_request'
content = {
    queue   : '' # the queue XREQ id
    heartbeat : '' # the heartbeat XREQ id
}

# msg_type = 'registration_reply'
content = {
    status : 'ok' # or 'error'
    # if ok:
    id : 0 # int, the engine id
    queue : 'tcp://127.0.0.1:12345' # the connection string for engine side of the queue
    heartbeat : (a,b) # tuple containing the two interfaces needed for heartbeat
    task : 'tcp...' # addr for task queue, or None if no task queue running
    # if error:
    reason : 'queue_id already registered'
}

# msg_type = 'connection_request'
content = {
}

# msg_type = 'connection_reply'
content = {
    status : 'ok' # or 'error'
    # if ok:
    queue : 'tcp://127.0.0.1:12345' # the connection string for the client side of the queue
    task : 'tcp...' # addr for task queue, or None if no task queue running
    controller : 'tcp...' # addr for controller methods, like queue_status, etc.
    # if error:
    reason : 'queue_id already registered' # str failure message
}

# msg_type = 'result_request'
content = {
    msg_id : uuid # str
}

# msg_type = '

# msg_type = 'result_reply'
content = {
    status : 'ok' # else error
    # if ok:
    'a-b-c-d' : msg # the content dict is keyed by msg_ids,
    ...             # values are the result messages
    # if error:
    reason : "explanation"
}

Controller PUB
--------------

# msg_type = 'registration_notification'
content = {
    id : 0 # engine ID that has been registered
}
# msg_type = 'unregistration_notification'
content = {
    id : 0 # engine ID that has been unregistered
}

Data
----
# msg_type = 'apply_message'
content = {
    'bound' : False # whether the message
}
buffers = [sf, sargs, skwargs, *data]

where buffers are built by streamsession.pack_apply_message(f,args,kwargs)
apply_messages can be unpacked with streamsession.unpack_apply_message(buffers)

# msg_type = 'apply_reply'
content = {
    'status' : 'ok'
}
buffers = [sobj, *data]

where buffers have been packed by streamsession.serialize_object(obj)
and can be reconstructed with: obj = streamsession.unserialize_object(buffers)

Controller Messages
-------------------

# msg_type = 'queue_status'
content = {
    'verbose' : True # whether return should be lists or lens
    'targets' : list of ints
}
# msg_type = 'queue_status'
content = {
    '0' : {'completed' : 1, 'queue' : 7}
}
# a dict




