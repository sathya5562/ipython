import zmq
import scheduler

class TaskMaster(object):
    """T"""
    
    def __init__(self, controller, tc_stream):
        self.controller = controller
        self.tc_stream = tc_stream
        self.scheduler = scheduler.Scheduler(self.controller)
        self.tc_stream.on_recv(self._handle_task_msg)
    
    def _handle_task_msg(self, msg):
        eid = self.scheduler.schedule()
        self.controller.engines[eid].send_multiple()