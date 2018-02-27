import time
from abc import ABCMeta, abstractmethod


class Poller(object):
    __metaclass__ = ABCMeta
    def __init__(self, polling_interval, get_latest_only=True):
        self.running = False
        self.polling_interval = polling_interval
        self.get_latest = get_latest_only

    @abstractmethod
    def do_work(self):
        raise NotImplementedError("Must override method: do_work")


    def start(self):
        self.running = True
        while self.running:
            start = time.clock()

            self.do_work()

            work_duration = time.clock() - start
            time.sleep(max(0, self.polling_interval - work_duration))

    def stop(self):
        self.running = False
        self.api_requests = None
