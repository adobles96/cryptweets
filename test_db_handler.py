import threading
import time


class TestDbHandlerThread(threading.Thread):

    def restart(self):
        for i in range(5):
            print("Hi from TestDbHandlerThread")
            time.sleep(5)
