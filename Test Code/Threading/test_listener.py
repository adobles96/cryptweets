import threading
import time


class TestListenerThread(threading.Thread):

    def __init__(self, e_go, e_wait, group=None, target=None, name=None,
                 args=(), *, daemon=None):
        super().__init__(group=group, target=target, name=name,
                         daemon=daemon)
        self.args = args
        self.e_go = e_go
        self.e_wait = e_wait

    def run(self):

        self.e_go.wait()

        while True:
            if not self.e_go.is_set():
                print("Listener: Told manager I stopped")
                self.e_wait.set()
                self.e_go.wait()
            print("Listener running")
            time.sleep(2)
