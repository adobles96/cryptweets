import threading
import time


def run():
    """
    This is the manager thread that will manage the listener (who is the thread that listens for Twitter data and sends
    this data to our database. It will also manage the db_handler, which is the thread that does clean up on our
    database after every cycle so we limit the amount of memory stored in our database.  Check our their docs for more
    details.
    :return: None
    """

    # Set threading events for communication with other threads.
    # e_gp is to tell the threads to go
    # e_wait is to wait for the thread to tell manager it has finished some job
    e_listener_go = threading.Event()
    e_listener_wait = threading.Event()
    e_handler_go = threading.Event()
    e_handler_wait = threading.Event()

    def listener_t():
        e_listener_go.wait()

        while True:
            if not e_listener_go.is_set():
                print("Listener: Told manager I stopped")
                e_listener_wait.set()
                e_listener_go.wait()
            print("Listener running")
            time.sleep(2)

    def handler_t():
        e_handler_go.wait()
        e_handler_go.clear()  # Make sure I only run once
        print("Handler: running, simulating my DB work for 5 seconds.")
        time.sleep(5)
        e_handler_wait.set()
        print("Handler: told main Im done with my work")

    # Set and start listener thread
    listener = threading.Thread(target=listener_t)
    listener.start()

    # Set and start handler thread
    handler = threading.Thread(target=handler_t)
    handler.start()

    while True:
        # Tell listener to run until we tell it to stop. Once we tell it to stop, we wait for it to tell us that it
        # stopped. Once listener tells us it stopped, we tell handler to go and we wait until it is done. Once its done
        # we repeat the cycle.

        print("Main: listener go!")
        e_listener_go.set()  # Tell listener to go
        print("Main: Simulate a day goes by, sleep for 10 seconds (24 hrs)")
        time.sleep(10)
        print("Main: listener stop!")
        e_listener_go.clear()  # Tell listener to stop
        print("Main: waiting for listener to stop")
        time.sleep(1)
        e_listener_wait.wait()  # Wait for listener to tell us it stopped
        print("Main: listener told me he stopped")
        e_listener_wait.clear()  # Clear the listener wait event so we can wait on next round
        print("Main: telling handler to go")
        e_handler_go.set()  # Tell handler to go
        print("Main: waiting for handler to say it finished")
        e_handler_wait.wait()  # Wait for handler to tell us it stopped
        print("Main: handler says its done, lets start again!")
        time.sleep(3)
        e_handler_wait.clear()  # Clear handler event so we can wait on next round


if __name__ == '__main__':
    run()
