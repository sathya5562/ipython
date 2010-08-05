"""RemoteNamespace object, for dict style interaction with a remote
execution kernel."""

from functools import wraps
def _clear():
    globals().clear()

class RemoteNamespace(object):
    """A RemoteNamespace object, providing dictionary 
    access to an engine via an IPython.zmq.client object.
    
    
    """
    client = None
    queue = None
    id = None
    block = False
    
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.block = client.block # initial state is same as client
    
    def __repr__(self):
        return "<RemoteNamespace[%i]>"%self.id
    
    def apply(self, f, *args, **kwargs):
        """call f(*args, **kwargs) in remote namespace
        
        This method has no access to the user namespace"""
        saveblock = self.client.block
        self.client.block = self.block
        result = self.client.apply_to(self.id, f, *args, **kwargs)
        self.client.block = saveblock
        return result
    
    def apply_bound(self, f, *args, **kwargs):
        """call `f(*args, **kwargs)` in remote namespace.
        
        `f` will have access to the user namespace as globals()."""
        saveblock = self.client.block
        self.client.block = self.block
        result = self.client.apply_bound_to(self.id, f, *args, **kwargs)
        self.client.block = saveblock
        return result
    
    def update(self, ns):
        """update remote namespace with dict `ns`"""
        return self.client.push(self.id, ns, self.block)
    
    def get(self, key_s):
        """get object(s) by `key_s` from remote namespace
        will return one object if it is a key.
        It also takes a list of keys, and will return a list of objects."""
        return self.client.pull(self.id, key_s, self.block)
    
    def clear(self):
        """clear the remote namespace"""
        return self.client.apply_bound_to(self.id, _clear)
    
    def schedule(self, toapply):
        """for use as a decorator, this turns a function into
        one that executes remotely."""
        @wraps(toapply)
        def applied(self, *args, **kwargs):
            return self.apply_bound_to(self, toapply, *args, **kwargs)
        return applied
        
        



