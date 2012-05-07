import sys
import unittest
import threading
import time

import zmq

import gtdoit.logbook
import gtdoit.messages.events_pb2 as events_pb2


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

        self.context = zmq.Context(3)

        self.transmitter = self.context.socket(zmq.PUSH)
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.setsockopt(zmq.SUBSCRIBE, '')

        self.transmitter.connect('tcp://127.0.0.1:8090')
        self.receiver.connect('tcp://127.0.0.1:8091')

        # Time it takes to connect. This is particularly important so that the
        # receiver does not just receive the tail of the stream.
        time.sleep(0.5)

        # Making sure context.term() does not time out
        # Could be removed if this test works as expected
        self.transmitter.setsockopt(zmq.LINGER, 1000)

    def testBasicEventProxying(self):
        new_event = events_pb2.TaskCreated(taskid='1',
                                           ownerid='2',
                                           name='Buy milk')
        event = events_pb2.Event(type=events_pb2.Event.TASK_CREATED,
                                 task_created=new_event)

        self.transmitter.send(event.SerializeToString())
        received_string = self.receiver.recv()

        received_event = events_pb2.Event()
        received_event.ParseFromString(received_string)

        self.assertEqual(received_event.type, events_pb2.Event.TASK_CREATED)
        self.assertEqual(received_event.task_created.taskid, '1')
        self.assertEqual(received_event.task_created.ownerid, '2')
        self.assertEqual(received_event.task_created.name, 'Buy milk')

    def testProxyingABunchOfEvents(self):
        NMESSAGES = 200

        # Sending
        new_event = events_pb2.TaskCreated()
        for i in range(NMESSAGES):
            new_event.taskid = '{0}'.format(2*i)
            new_event.ownerid = '{0}'.format(i+1)
            new_event.name = 'Buy milk number {0}'.format(i)

            event = events_pb2.Event(type=events_pb2.Event.TASK_CREATED,
                                     task_created=new_event)
            self.transmitter.send(event.SerializeToString())

        # Receiving and asserting correct messages
        received_event = events_pb2.Event()
        for i in range(NMESSAGES):
            received_string = self.receiver.recv()

            received_event.ParseFromString(received_string)

            self.assertEqual(received_event.type, events_pb2.Event.TASK_CREATED)
            print i, received_event.task_created.taskid
            self.assertEqual(received_event.task_created.taskid,
                             '{0}'.format(2*i))
            self.assertEqual(received_event.task_created.ownerid,
                             '{0}'.format(i+1))
            self.assertEqual(received_event.task_created.name,
                             'Buy milk number {0}'.format(i))

    def testMessageOrdering(self):
        """Making sure that messages are received in the correct order"""
        # TODO: Write
        pass

    def tearDown(self):
        self.transmitter.close()
        self.receiver.close()

        self.assertTrue(self.logbook.isAlive(),
                        "Did logbook crash? Not running.")
        socket = self.context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect('tcp://127.0.0.1:8090')
        socket.send('EXIT')
        time.sleep(0.5) # Acceptable exit time
        self.assertFalse(self.logbook.isAlive())
        socket.close()

        self.context.term()

