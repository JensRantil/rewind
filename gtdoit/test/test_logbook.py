import sys
import unittest
import threading
import time
import uuid
import random

import zmq

import gtdoit.logbook
import gtdoit.messages.events_pb2 as events_pb2
import gtdoit.communicators as communicators


class TestEventStore(unittest.TestCase):
    def setUp(self):
        self.keys = [str(i) for i in range(10)]
        self.vals = [str(i+30) for i in range(10)]
        self.store = gtdoit.logbook.EventStore()
        for key, val in zip(self.keys, self.vals):
            self.store.add_event(key, val)

    def testQueryingAll(self):
        result = self.store.get_events()
        self.assertEqual(list(result), self.vals)

    def testQueryAfter(self):
        result = self.store.get_events(from_=self.keys[0])
        self.assertEqual(list(result), self.vals[1:])
        result = self.store.get_events(from_=self.keys[1])
        self.assertEqual(list(result), self.vals[2:])

    def testQueryBefore(self):
        result = self.store.get_events(to=self.keys[-1])
        self.assertEqual(list(result), self.vals)
        result = self.store.get_events(to=self.keys[-2])
        self.assertEqual(list(result), self.vals[:-1])

    def testQueryBetween(self):
        result = self.store.get_events(from_=self.keys[1], to=self.keys[-2])
        self.assertEqual(list(result), self.vals[2:-1])


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


class TestLogbookReplication(unittest.TestCase):
    def setUp(self):
        args = ['--exit-codeword', 'EXIT',
                '--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = threading.Thread(target=gtdoit.logbook.main,
                                        name="logbook-replication-test",
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
        self.assertTrue(self.logbook.isAlive(), "Did logbook crash? Not running.")
        socket = self.context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect('tcp://127.0.0.1:8090')
        socket.send('EXIT')
        time.sleep(0.5) # Acceptable exit time
        self.assertFalse(self.logbook.isAlive())
        socket.close()

        self.context.term()


class TestLogbookQuerying(unittest.TestCase):
    def setUp(self):
        args = ['--exit-codeword', 'EXIT',
                '--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--query-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = threading.Thread(target=gtdoit.logbook.main,
                                        name="logbook-querying-test",
                                        args=(args,), kwargs={'exit': False})
        self.logbook.start()

        self.context = zmq.Context(3)

        self.query_socket = self.context.socket(zmq.REQ)
        self.query_socket.connect('tcp://127.0.0.1:8091')
        self.querier = communicators.EventQuerier(self.query_socket)

        transmitter = self.context.socket(zmq.PUSH)
        transmitter.connect('tcp://127.0.0.1:8090')

        # Making sure context.term() does not time out
        # Could be removed if this test works as expected
        transmitter.setsockopt(zmq.LINGER, 1000)

        ids = [unicode(uuid.uuid1()) for i in range(200)]
        self.assertEqual(len(ids), len(set(ids)), 'There were duplicate IDs.'
                         ' Maybe the UUID1 algorithm is flawed?')
        users = [unicode(uuid.uuid1()) for i in range(30)]
        self.assertEqual(len(users), len(set(users)),
                         'There were duplicate users.'
                         ' Maybe the UUID1 algorithm is flawed?')

        self.sent = []
        for id in ids:
            new_event = events_pb2.TaskCreated()
            new_event.taskid = id
            new_event.ownerid = random.choice(users)
            new_event.name = 'Buy flowers to Julie'
            # TODO: This is too tedious. Encapsulate using a class.
            event = events_pb2.Event(type=events_pb2.Event.TASK_CREATED,
                                     task_created=new_event)
            self.sent.append(event)
            transmitter.send(event.SerializeToString())
        transmitter.close()

    def _getAllPastEvents(self):
        """Get all past events.

        Helper class function to get the eventids of the events.

        This function is tested through testSyncAllPastEvents(...).
        """
        return [event for event in self.querier.query()]

    def testSyncAllPastEvents(self):
        time.sleep(0.5) # Max time to persist the messages
        events = self._getAllPastEvents()

        self.assertEqual(events[0].event.task_created.taskid,
                         self.sent[0].task_created.taskid,
                         'Initial elements are not the same')
        self.assertEqual(len(events), len(self.sent))
        for received_stored_event, sent in zip(events, self.sent):
            received = received_stored_event.event

            self.assertEqual(received.type, sent.type)

            recvd = received.task_created
            sentd = sent.task_created

            self.assertEqual(recvd.taskid, sentd.taskid)
            self.assertEqual(recvd.ownerid, sentd.ownerid)
            self.assertEqual(recvd.name, sentd.name)

    def testSyncEventsSince(self):
        time.sleep(0.5) # Max time to persist the messages
        allevents = self._getAllPastEvents()
        from_ = allevents[3].eventid
        events = [event for event in self.querier.query(from_=from_)]
        self.assertEqual(allevents[4:], events)
        
    def testSyncEventsBefore(self):
        time.sleep(0.5) # Max time to persist the messages
        allevents = self._getAllPastEvents()
        to = allevents[-3].eventid
        events = [event for event in self.querier.query(to=to)]
        self.assertEqual(allevents[:-2], events)

    def testSyncEventsBetween(self):
        time.sleep(0.5) # Max time to persist the messages
        allevents = self._getAllPastEvents()
        from_ = allevents[3].eventid
        to = allevents[-3].eventid
        events = [event for event in self.querier.query(from_=from_, to=to)]
        self.assertEqual(allevents[4:-2], events)

    def tearDown(self):
        self.query_socket.close()

        self.assertTrue(self.logbook.isAlive(), "Did logbook crash? Not running.")
        socket = self.context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect('tcp://127.0.0.1:8090')
        socket.send('EXIT')
        time.sleep(0.5) # Acceptable exit time
        self.assertFalse(self.logbook.isAlive())
        socket.close()

        self.context.term()
