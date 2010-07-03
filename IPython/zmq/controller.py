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

import zmq

#-----------------------------------------------------------------------------
# Code
#-----------------------------------------------------------------------------

class Controller(object):
    """The IPython Controller with 0MQ connections"""
    def __init__(self, ):
        """"""