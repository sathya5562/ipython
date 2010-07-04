import os
import uuid
import pprint

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

unpack = pickle.loads
# the message types:
MSG_TYPES = "relay failure query query_result relay_result".split()


def send_split_message(stream, packer, msg):
    """send a message packed in two parts"""
    parts = []
    for sec in msg:
        if not isinstance(sec, str):
            sec = packer(sec)
        parts.append(sec)
    return stream.send_multipart(parts)

def send_message_json(stream, msg):
    """send a message in two parts with send_split_message(),
    using json.dumps(...separators=(',',':')) as the packer"""
    packer = lambda o: json.dumps(o, separators=(',',':'))
    return send_split_message(stream, packer, msg)

def send_message_pickle(stream, msg):
    """send a message in two parts with send_split_message(),
    using pickle.dumps(...separators=(',',':')) as the packer"""
    packer = lambda o: pickle.dumps(o,2)
    return send_split_message(stream, packer, msg)

def unpack_message(msg):
    parts = []
    for sec in msg:
        try:
            sec = unpack(sec)
        except:
            pass
        parts.append(sec)
    return parts
    
class Message(object):
    """A simple message object that maps dict keys to attributes.

    A Message can be created from a dict and a dict from a Message instance
    simply by calling dict(msg_obj)."""
    
    def __init__(self, msg_dict):
        dct = self.__dict__
        for k, v in msg_dict.iteritems():
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
    
    def get(self, k,default=None):
        return self.__dict__.get(k,default)


def msg_header(msg_id, msg_type):
    return {
        'msg_id' : msg_id,
        'msg_type' : msg_type
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

