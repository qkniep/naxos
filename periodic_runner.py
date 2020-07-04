import threading
import time
import heapq as hp
from datetime import datetime, timedelta


class PeriodicRunner(threading.Thread):

    def __init__(self):
        super().__init__()

        self.functions = []
        self.contexts = []
        self.pq = []
        self.running = True

    def register(self, func, context, timing):
        """Register a function func to be run each timing seconds, passing context as a parameter that can keep state"""
        if type(timing) is int:  # convert if timing is an int of seconds
            timing = timedelta(seconds=timing)
        now = datetime.now()
        self.functions.append((timing, func))
        self.contexts.append(context)
        hp.heappush(self.pq, (now+timing, len(self.functions)-1))

    def unregister(self, func):  # TODO: Thread safe. Locks?
        print("Unregister function %s" % func.__name__)
        if len(self.functions) == 0:
            print("No functions registered.")
            return
        index = -1
        for i, t in enumerate(self.functions):
            if t is not None and func is t[1]:
                index = i
                break
        if index == -1:
            print("Functions %s not found" % func.__name__)
            return
        #  keep fields, so that the pq doesn't have to be updated.
        self.contexts[index] = None
        self.functions[index] = None

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            if len(self.pq) == 0:
                continue
            (t, index) = hp.heappop(self.pq)
            if self.functions[index] is None:  # handle unregistered functions
                continue
            (timing, func) = self.functions[index]
            context = self.contexts[index]

            delta = (t - datetime.now()).total_seconds()  # amount of time we have to sleep in seconds
            if delta < 0:  # if it is negative, we missed the timeslot
                print("Couldn't keep up! Missed running %s it by %s s" % (func.__name__, abs(delta)))
            else:
                time.sleep(delta)
            # execute the function, then readd it to the pq
            if self.functions[index] is None:  # could have been unregistered while we were sleeping, so we have to check again.
                continue
            func(context)
            hp.heappush(self.pq, (datetime.now()+timing, index))
