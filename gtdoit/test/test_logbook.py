import sys
import unittest
import threading
import time

import zmq

import gtdoit.logbook


class TestArgumentParsing(unittest.TestCase):
    """Tests command line arguments to `logbook`.

    TODO: Test the 'PROG --help' call gives expected output. Don't know how to
          override sys.exit in best way.
    """
    def testAtLeastOneEndpointRequired(self):
        exitcode = gtdoit.logbook.main([], exit=False)
        self.assertEqual(exitcode, 2)

    def testOnlyStreamingEndpointFails(self):
        exitcode = gtdoit.logbook.main(['--streaming-bind-endpoint',
                                        'tcp://hello'], exit=False)
        self.assertEqual(exitcode, 2)


class TestLogbook(unittest.TestCase):
    def setUp(self):
        args = ['--exit-codeword', 'EXIT',
                '--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = threading.Thread(target=gtdoit.logbook.main,
                                        name="logbook-test",
                                        args=(args,), kwargs={'exit': False})
        self.logbook.start()
        self.context = zmq.Context(1)

    def testThis(self):
        # TODO: Make sure that events are being replicated
        pass

    def tearDown(self):
        socket = self.context.socket(zmq.PUSH)
        socket.connect('tcp://127.0.0.1:8090')
        socket.send('EXIT')
        time.sleep(0.5) # Acceptable exit time
        self.assertFalse(self.logbook.isAlive())

        socket.close()
        self.context.term()

