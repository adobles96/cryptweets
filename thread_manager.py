
import threading
from test_listener import TestListenerThread
from test_db_handler import TestDbHandlerThread
import time


def run():
    e = threading.Event()
    e2 = threading.Event()
    listener = TestListenerThread(e=e, e2=e2)
    listener.start()

    for i in range(5):
        print("Main sleeps for a second and tells listener to go. Waits for listener.")
        time.sleep(1)
        e.set()
        e2.wait()
        e2.clear()


if __name__ == '__main__':
    run()
