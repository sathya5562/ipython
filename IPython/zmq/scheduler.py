#!/usr/bin/env python
# encoding: utf-8

"""The IPython Kernel with 0MQ
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

import random

#-----------------------------------------------------------------------------
# Code
#-----------------------------------------------------------------------------


def weighted_twobin(weights):
    """given a list of weights, roll two weighted dice, and pick the 
    one with the higher weight."""
    assert(sum(weights) > 0)
    assert(min(weights) >= 0)
    N = sum(weights)
    r = random.randint(0,N-1)
    idx = 0
    s = weights[idx]
    while not r < s:
        idx += 1
        s += weights[idx]
    first = idx
    print N,r,first
    second = first
    while second == first:
        r = random.randint(0,N-1)
        idx = 0
        s = weights[idx]
        while not r < s:
            idx += 1
            s += weights[idx]
        second = idx
        print N,r,second
    
    wa = weights[first]
    wb = weights[second]
    
    if wa >= wb:
        return first
    
    return second
    

class Scheduler(object):
    """A Scheduler object for tasking"""
    
    controller = None
    
    def __init__(self, controller):
        self.controller = controller
    
    
    def schedule(self):
        loads = self.controller.update_loads()
        N = sum(loads)
        r = random.randint(0,N-1)
        
        