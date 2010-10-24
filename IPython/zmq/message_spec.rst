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
StreamSession.send()

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

Control
-------

# msg_type = 'heartbeat'
content = {

}

Relay
-----

# msg_type = 'relay_request'
subheader = {
    targets: [0,2,5] # None|int|list of ints
    submsg_type: 'execute_request' # the msg_type of the message to be relayed
}

# msg_type = 'relay_success'
content = {
    relay_ids: [1,2,3] # list of ints specifying on which engines job was executed
}