#!/usr/bin/env python
"""edited session.py to work with streams, and move msg_type to the header
"""


import os
import sys
import traceback
import pprint
import uuid

import zmq

try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json = None

try:
    import cPickle as pickle
except:
    import pickle

# packers

json_packer = lambda o: json.dumps(o, separators=(',',':'))

pickle_packer = lambda o: pickle.dumps(o, 2)

if json is not None:
    default_packer = json_packer
    default_unpacker = json.loads
else:
    default_packer = pickle_packer
    default_unpacker = pickle.loads

def wrap_exception():
    etype, evalue, tb = sys.exc_info()
    tb = traceback.format_exception(etype, evalue, tb)
    exc_content = {
        u'status' : u'error',
        u'traceback' : tb,
        u'etype' : unicode(etype),
        u'evalue' : unicode(evalue)
    }
    return exc_content

class Message(object):
    """A simple message object that maps dict keys to attributes.

    A Message can be created from a dict and a dict from a Message instance
    simply by calling dict(msg_obj)."""
    
    def __init__(self, msg_dict):
        dct = self.__dict__
        for k, v in dict(msg_dict).iteritems():
            if isinstance(v, dict):
                v = Message(v)
            dct[k] = v

    # Having this iterator lets dict(msg_obj) work out of the box.
    def __iter__(self):
        return iter(self.__dict__.iteritems())
    
    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def __contains__(self, k):
        return k in self.__dict__

    def __getitem__(self, k):
        return self.__dict__[k]


def msg_header(msg_id, msg_type, username, session):
    return {
        'msg_id' : msg_id,
        'msg_type': msg_type,
        'username' : username,
        'session' : session
    }


def extract_header(msg_or_header):
    """Given a message or header, return the header."""
    if not msg_or_header:
        return {}
    try:
        # See if msg_or_header is the entire message.
        h = msg_or_header['header']
    except KeyError:
        try:
            # See if msg_or_header is just the header
            h = msg_or_header['msg_id']
        except KeyError:
            raise
        else:
            h = msg_or_header
    if not isinstance(h, dict):
        h = dict(h)
    return h


class StreamSession(object):
    """tweaked version of IPython.zmq.session.session, for use with pyzmq ZMQStreams"""

    def __init__(self, username=os.environ.get('USER','username'), session=None, packer=None,unpacker=None):
        self.username = username
        if session is None:
            self.session = str(uuid.uuid4())
        else:
            self.session = session
        self.msg_id = str(uuid.uuid4())
        if packer is None:
            self.pack = default_packer
        else:
            self.pack = packer
        
        if unpacker is None:
            self.unpack = default_unpacker
        else:
            self.unpack = unpacker
        
        self.none = self.pack({})
            
    def msg_header(self, msg_type):
        h = msg_header(self.msg_id, msg_type, self.username, self.session)
        self.msg_id = str(uuid.uuid4())
        return h

    def msg(self, msg_type, content=None, parent=None, subheader=None):
        msg = {}
        msg['header'] = self.msg_header(msg_type)
        msg['msg_id'] = msg['header']['msg_id']
        msg['parent_header'] = {} if parent is None else extract_header(parent)
        msg['msg_type'] = msg_type
        msg['content'] = {} if content is None else content
        sub = {} if subheader is None else subheader
        msg['header'].update(sub)
        return msg

    def send(self, stream, msg_type, content=None, subheader=None, parent=None, ident=None):
        """send a message via stream"""
        msg = self.msg(msg_type, content, parent, subheader)
        to_send = []
        if ident is not None:
            to_send.append(ident)
        to_send.append(self.pack(msg['header']))
        to_send.append(self.pack(msg['parent_header']))
        # if parent is None:
        #     to_send.append(self.none)
        # else:
        #     to_send.append(self.pack(dict(parent)))
        if content is None:
            content = self.none
        elif isinstance(content, dict):
            content = self.pack(content)
        elif isinstance(content, str):
            # content is already packed, as in a relayed message
            pass
        else:
            raise TypeError("Content incorrect type: %s"%type(content))
        to_send.append(content)
        stream.send_multipart(to_send)
        omsg = Message(msg)
        return omsg

    def recv(self, socket, mode=zmq.NOBLOCK, content=True):
        """"""
        try:
            msg = socket.recv_multipart(mode)
        except zmq.ZMQError, e:
            if e.errno == zmq.EAGAIN:
                # We can convert EAGAIN to None as we know in this case
                # recv_json won't return None.
                return None
            else:
                raise
        # return an actual Message object
        try:
            return self.unpack_message(msg[-3:], content)
        except Exception, e:
            # TODO: handle it
            raise e
    
    def unpack_message(self, msg, content=True):
        """return a message object from the format
        sent by self.send"""
        assert len(msg) == 3, "malformed message"
        message = {}
        message['header'] = self.unpack(msg[0])
        message['msg_type'] = message['header']['msg_type']
        message['parent_header'] = self.unpack(msg[1])
        if content:
            message['content'] = self.unpack(msg[-1])
        else:
            message['content'] = msg[-1]
        
        return message
            
        

def test_msg2obj():
    am = dict(x=1)
    ao = Message(am)
    assert ao.x == am['x']

    am['y'] = dict(z=1)
    ao = Message(am)
    assert ao.y.z == am['y']['z']
    
    k1, k2 = 'y', 'z'
    assert ao[k1][k2] == am[k1][k2]
    
    am2 = dict(ao)
    assert am['x'] == am2['x']
    assert am['y']['z'] == am2['y']['z']
