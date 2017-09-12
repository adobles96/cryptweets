import threading
import time


class TestDbHandlerThread(threading.Thread):

    def __init__(self, e_go, e_wait, group=None, target=None, name=None,
                 args=(), *, daemon=None):
        super().__init__(group=group, target=target, name=name,
                         daemon=daemon)
        self.args = args
        self.e_go = e_go
        self.e_wait = e_wait

    def run(self):
        while True:
            self.e_go.wait()
            self.e_go.clear()  # Make sure I only run once
            print("Handler: running, simulating my DB work for 5 seconds.")
            time.sleep(5)
            self.e_wait.set()
            print("Handler: told main Im done with my work")
