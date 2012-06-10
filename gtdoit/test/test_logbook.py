import sys
import unittest
import threading
import time
import uuid
import random
import tempfile
import os
import shutil
import itertools

import zmq

import gtdoit.logbook
import gtdoit.messages.events_pb2 as events_pb2
import gtdoit.communicators as communicators


class _TestEventStore(unittest.TestCase):
    """Test a generic event store.

    This class is abstract and should be subclassed in a class that defines a
    setUp(self) class function.
    """
    def _populate_store(self):
        """Helper method to populate the store.

        The keys and values that were put in the store are saved to self.keys
        and self.vals.
        """
        # Randomizing here mostly because rotation will behave differently
        # depending on the number of generated events.
        N = random.randint(10, 29)

        # Important to print this (for test reproducability) since N is
        # random.
        print "Populating with {0} events...".format(N)
        self.keys = [str(i) for i in range(N)]
        self.vals = [str(i+30) for i in range(N)]
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

    def testKeyExists(self):
        for key in self.keys:
            self.assertTrue(self.store.key_exists(key),
                            "Key did not exist: {0}".format(key))


class TestPersistedEventStore(_TestEventStore):
    """Test `PersistedEventStore`."""
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='test_logbook',
                                        suffix='persisted_event_store')
        self.store = gtdoit.logbook.PersistedEventStore(self.tempdir)
        self._populate_store()

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tempdir)

    def testReopening(self):
        events_before_reload = self.store.get_events()
        self.store.close()
        self.store = gtdoit.logbook.PersistedEventStore(self.tempdir)
        events_after_reload = self.store.get_events()
        self.assertEqual(list(events_before_reload), list(events_after_reload))


class TestRotatedPersistedEventStore(TestPersistedEventStore):
    """ Tests `PersistedEventStore`, but makes sure to rotate it a couple of
        times before running the tests.
    """
    def setUp(self):
        """Called before each test.

        Deliberately overriding `TestPersistedEventStore.setUp()`.
        """
        N = 5
        self.N = N
        self.tempdir = tempfile.mkdtemp(prefix='test_logbook',
                                        suffix='persisted_event_store')
        self.store = gtdoit.logbook.PersistedEventStore(self.tempdir,
                                                        events_per_batch=N)
        self._populate_store()

    def testReopening(self):
        """Testing reopening and appending a couple of more events."""
        self.store.close()
        self.store = gtdoit.logbook.PersistedEventStore(self.tempdir,
                                                        events_per_batch=self.N)

        N_MORE = 20
        more_keys = ['a{0}'.format(i) for i in range(N_MORE)]
        more_vals = ['b{0}'.format(i) for i in range(N_MORE)]
        for k,v in zip(more_keys, more_vals):
            self.store.add_event(k, v)
        events = list(self.store.get_events())
        events_expected = list(itertools.chain(self.vals, more_vals))
        self.assertEqual(events, events_expected)

    def testKeyExists(self):
        """Test that key_exists only checks against the last event batch."""
        # Number of events in last batch
        nlastbatch = len(self.keys) % self.N
        if nlastbatch == 0:
            nlastbatch = self.N
        # Number of events in the other batches
        nprevbatches = len(self.keys) - nlastbatch
        self.assertTrue(nlastbatch > 0,
                        "Wanted to have some events in the last batch")
        self.assertTrue(nprevbatches > 0,
                        "Expected previous batches")

        # Can be used for debugging of the contents of a log file.
        #to_execute = 'head -n 100 {0}/logs/*'.format(self.tempdir)
        #os.system(to_execute)

        for key in self.keys[-nlastbatch:]:
            self.assertTrue(self.store.key_exists(key),
                            'Key did not exist: %s' % key)
        for key in self.keys[:nprevbatches]:
            self.assertFalse(self.store.key_exists(key),
                             "Key existed when it shouldn't: %s" % key)


class TestLogEventStore(_TestEventStore):
    def setUp(self):
        self.tempfile = tempfile.NamedTemporaryFile(prefix='test_logbook',
                                                    suffix='.log',
                                                    delete=False)
        self.tempfile.close() # We are not to modify it directly
        self.store = gtdoit.logbook._LogEventStore(self.tempfile.name)
        
        self._populate_store()
    
    def testReopenWithClose(self):
        self.store.close()
        self.store = gtdoit.logbook._LogEventStore(self.tempfile.name)
        self.assertEqual(len(self.keys), len(self.vals),
                        "Keys and vals did not match in number.")
        self.assertEqual(len(self.store.get_events(),), len(self.keys))

    def testReopenWithoutClose(self):
        self.store = gtdoit.logbook._LogEventStore(self.tempfile.name)
        self.assertEqual(len(self.keys), len(self.vals),
                        "Keys and vals did not match in number.")
        self.assertEqual(len(self.store.get_events(),), len(self.keys))

    def tearDown(self):
        self.store.close()
        os.remove(self.tempfile.name)


class TestSQLiteEventStore(_TestEventStore):
    """Test `_SQLiteEventStore`."""
    def setUp(self):
        self.tempfile = tempfile.NamedTemporaryFile(prefix='test_logbook',
                                                    suffix='sqlite_evstore',
                                                    delete=False)
        self.tempfile.close() # We are not to modify it directly
        self.store = gtdoit.logbook._SQLiteEventStore(self.tempfile.name)
        
        self._populate_store()

    def testCount(self):
        self.assertEqual(len(self.keys), len(self.vals),
                        "Keys and vals did not match in number.")
        self.assertTrue(self.store.count() == len(self.keys),
                        "Count was incorrect.")

    def testReopenWithoutClose(self):
        self.store = gtdoit.logbook._SQLiteEventStore(self.tempfile.name)

        # testCount does exactly the test we want to do. Reusing it.
        self.testCount()

    def testReopenWithClose(self):
        self.store.close()
        self.store = gtdoit.logbook._SQLiteEventStore(self.tempfile.name)

        # testCount does exactly the test we want to do. Reusing it.
        self.testCount()

    def tearDown(self):
        self.store.close()
        os.remove(self.tempfile.name)
        

class TestInMemoryEventStore(_TestEventStore):
    """Test `InMemoryEventStore`."""
    def setUp(self):
        self.store = gtdoit.logbook.InMemoryEventStore()
        self._populate_store()

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
        """Tests that a bunch of incoming messages processed correctly.

        That is, they are all being proxied and in order.
        """
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
