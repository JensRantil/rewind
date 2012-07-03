import contextlib
import itertools
import random
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid
import os

import mock
import zmq

import gtdoit.communicators as communicators
import gtdoit.logbook
import gtdoit.messages.events_pb2 as events_pb2


class _TestEventStore:
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


class TestEventStore(unittest.TestCase):
    """Tests the class `EventStore`."""
    def testStubs(self):
        """Makes sure `EventStore` behaves the way we expect."""
        estore = gtdoit.logbook.EventStore()
        self.assertRaises(NotImplementedError, estore.add_event, "key", "event")
        self.assertRaises(NotImplementedError, estore.get_events)
        self.assertRaises(NotImplementedError, estore.get_events, "from")
        self.assertRaises(NotImplementedError, estore.get_events, "from", "to")
        self.assertRaises(NotImplementedError, estore.key_exists, "key")
        estore.close() # Should not throw anything


class TestRotationEventStore(unittest.TestCase, _TestEventStore):
    """Test `RotationEventStore`."""

    # Number of events per batch
    EVS_PER_BATCH = 5

    def setUp(self):
        rotated_estore_params = [
            {
                'dirpath': '/notused/db',
                'prefix': 'logdb',
            },
            {
                'dirpath': '/notused/log',
                'prefix': 'appendlog',
            },
        ]

        memstores = []
        rotated_memstores = []
        files_created = {}
        for params in rotated_estore_params:
            def memstore_factory_side_effect(fname):
                if fname not in files_created:
                    memstore = gtdoit.logbook.InMemoryEventStore()
                    memstore.close = mock.Mock()
                    memstores.append(memstore)
                    files_created[fname] = memstore
                return files_created[fname]
            memstore_factory = mock.Mock()
            memstore_factory.side_effect = memstore_factory_side_effect
            with mock.patch('os.mkdir') as mkdir_mock:
                rotated_memstore = \
                        gtdoit.logbook.RotatedEventStore(memstore_factory,
                                                         **params)
                mkdir_mock.assert_called_once()

            rotated_memstores.append(rotated_memstore)

        self.memstores = memstores
        self.rotated_memstores = rotated_memstores
        self.files_created = files_created

        self._openStore()
        self._populate_store()

    def _openStore(self):
        evs_per_batch = TestRotationEventStore.EVS_PER_BATCH
        store = gtdoit.logbook.RotationEventStore(evs_per_batch)
        for rotated_memstore in self.rotated_memstores:
            store.add_rotated_store(rotated_memstore)
        self.store = store

    def tearDown(self):
        self.store.close()
        for memstore in self.memstores:
            memstore.close.assert_called_once()

    def testReopening(self):
        events_before_reload = self.store.get_events()
        self.store.close()
        self._openStore()
        events_after_reload = self.store.get_events()
        self.assertEqual(list(events_before_reload), list(events_after_reload))
        
    def testKeyExists(self):
        evs_per_batch = TestRotationEventStore.EVS_PER_BATCH
        nkeys_in_last_batch = len(self.keys) % evs_per_batch
        if nkeys_in_last_batch > 0:
            # No reasons to test if there were no events written to this batch
            keys_in_last_batch = self.keys[-nkeys_in_last_batch:]
            for key in keys_in_last_batch:
                self.assertTrue(self.store.key_exists(key),
                                "Key did not exist: {0}".format(key))


class TestRotatedEventStorage(unittest.TestCase, _TestEventStore):
    def setUp(self):
        """Setup method before each test.

        TODO: Use loops instead of suffixed variables.
        """
        N = 20

        mstore1 = gtdoit.logbook.InMemoryEventStore()
        mstore1.close = mock.MagicMock() # Needed for assertions
        keys1 = [str(i) for i in range(N)]
        vals1 = [str(i+30) for i in range(N)]
        for key, val in zip(keys1, vals1):
            mstore1.add_event(key, val)

        mstore2 = gtdoit.logbook.InMemoryEventStore()
        mstore2.close = mock.MagicMock() # Needed for assertions
        keys2 = [str(i+N) for i in range(N)]
        vals2 = [str(i+30+N) for i in range(N)]
        for key, val in zip(keys2, vals2):
            mstore2.add_event(key, val)

        mstore3 = gtdoit.logbook.InMemoryEventStore()
        mstore3.close = mock.MagicMock() # Needed for assertions
        keys3 = ['one', 'two', 'three']
        vals3 = ['four', 'five', 'six']
        for key, val in zip(keys3, vals3):
            mstore3.add_event(key, val)

        mstore4 = gtdoit.logbook.InMemoryEventStore()

        def es_factory(fname):
            """Pretends to open an event store from a filename."""
            retvals = {
                '/random_dir/eventdb.0': mstore1,
                '/random_dir/eventdb.1': mstore2,
                '/random_dir/eventdb.2': mstore3,
                '/random_dir/eventdb.3': mstore4,
            }
            return retvals[fname]
        estore_factory = mock.Mock(side_effect=es_factory)

        with mock.patch('os.path.exists') as exists_mock, \
                mock.patch('os.listdir') as listdir_mock:
            exists_mock.return_value = True
            listdir_mock.return_value = ['eventdb.0', 'eventdb.1', 'eventdb.2']
            store = gtdoit.logbook.RotatedEventStore(estore_factory,
                                                     '/random_dir',
                                                     'eventdb')
            exists_mock.assert_called_with('/random_dir')
            self.assertTrue(listdir_mock.call_count > 0)

        estore_factory.assert_called_once_with('/random_dir/eventdb.2')

        self.assertEqual(store.batchno, 2)

        # Test attributes
        self.store = store
        self.keys = keys1 + keys2 + keys3
        self.vals = vals1 + vals2 + vals3
        self.keys3, self.vals3 = keys3, vals3
        self.estore_factory = estore_factory
        self.mstore1 = mstore1
        self.mstore2 = mstore2
        self.mstore3 = mstore3
        self.mstore4 = mstore4

    def testRotation(self):
        self.mstore2.close.reset_mock()
        self.estore_factory.reset_mock()

        self.store.rotate()

        # Making sure we closed and opened the right event store
        self.mstore3.close.assert_called_once_with()
        self.estore_factory.assert_called_once_with('/random_dir/eventdb.3')

    def testWritingAfterRotation(self):
        """Test writing to the rotated event store after rotation."""
        self.store.rotate()

        self.assertFalse(self.store.key_exists('mykey'))
        self.store.add_event('mykey', 'myvalue')
        self.assertTrue(self.store.key_exists('mykey'),
                        "The event was expected to have been written.")
        self.assertTrue(self.mstore4.key_exists('mykey'),
                        "The event seem to have been written to wrong estore.")

    def testKeyExists(self):
        """Testing RotatedEventStore.key_exists(...).
        
        Overriding this test, because RotatedEventStore.key_exists(...) only
        checks the last batch.
        """
        for key in self.keys3:
            self.assertTrue(self.store.key_exists(key),
                            "Key did not exist: {0}".format(key))


class TestLogEventStore(unittest.TestCase, _TestEventStore):
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


class TestSQLiteEventStore(unittest.TestCase, _TestEventStore):
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
        

class TestInMemoryEventStore(unittest.TestCase, _TestEventStore):
    """Test `InMemoryEventStore`."""
    def setUp(self):
        self.store = gtdoit.logbook.InMemoryEventStore()
        self._populate_store()


@contextlib.contextmanager
def _direct_stderr_to_stdout():
    """Context manager for wrapping tests that prints to stderr.
    
    Nosetests does not capture stderr.
    """
    real_stderr = sys.stderr
    sys.stderr = sys.stdout
    yield
    sys.stderr = real_stderr


class TestCommandLineExecution(unittest.TestCase):
    """Tests various command line arguments for `logbook`."""
    def setUp(self):
        # See tearDown() why this one is defined
        self.logbook = None

    def tearDown(self):
        if self.logbook and self.logbook.isAlive():
            # Making sure to close a logbook if it has been defined
            self.logbook.stop()
            self.logbook = None

    def testAtLeastOneEndpointRequired(self):
        with _direct_stderr_to_stdout():
            logbook = _LogbookThread([])
            logbook.start()
            logbook.join(2)
        self.assertFalse(logbook.isAlive())
        self.assertEqual(logbook.exit_code, 2)

    def testOnlyStreamingEndpointFails(self):
        with _direct_stderr_to_stdout():
            logbook = _LogbookThread(['--streaming-bind-endpoint', 'tcp://hello'])
            logbook.start()
            logbook.join(2)
        self.assertFalse(logbook.isAlive())
        self.assertEqual(logbook.exit_code, 2)

    def testHelp(self):
        with _direct_stderr_to_stdout():
            logbook = _LogbookThread(['--help'])
            logbook.start()
            logbook.join(2)
        self.assertFalse(logbook.isAlive())
        self.assertEqual(logbook.exit_code, 0)

    def testStartingWithPersistence(self):
        datapath = tempfile.mkdtemp()
        print "Using datapath:", datapath

        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091',
                '--datadir', datapath]
        print " ".join(args)
        self.logbook = _LogbookThread(args, 'tcp://127.0.0.1:8090')
        self.logbook.start()

        time.sleep(3)
        self.assertTrue(self.logbook.isAlive(),
                        "The logbook was not running for more than 3 seconds")

        # Not removing this in tearDown for two reasons:
        # 1. Datapath is not created in setUp()
        # 2. If this test fails, we will keep the datapath that was created.
        shutil.rmtree(datapath)


class _LogbookThread(threading.Thread):
    """A thread that runs a logbook instance.

    While the thread is given command line arguments, the logbook is started as
    thread rather than external process. This makes it possible to check code
    coverage and track exit codes etc.
    """
    _EXIT_CODE = 'EXIT'

    def __init__(self, cmdline_args, exit_addr=None):
        """Constructor.

        Parameters:
        cmdline_args -- command line arguments used to execute the logbook.
        exit_addr    -- the ZeroMQ address used to send the exit message to.
        """
        thread = self

        assert '--exit-codeword' not in cmdline_args, \
                "'--exit-codeword' is added by _LogbookThread. Not elsewhere"
        cmdline_args = ['--exit-codeword', _LogbookThread._EXIT_CODE] + cmdline_args

        def exitcode_runner(*args, **kwargs):
            try:
                thread.exit_code = gtdoit.logbook.main(*args, **kwargs)
            except SystemExit as e:
                thread.exit_code = e.code
            else:
                # If SystemExit is never thrown Python would have exitted with
                # exit code 0
                thread.exit_code = 0
        super(_LogbookThread, self).__init__(target=exitcode_runner,
                                             name="test-logbook",
                                             args=(cmdline_args,))
        self._exit_addr = exit_addr

    def stop(self, context=None):
        """Send a stop message to the event thread."""
        assert self._exit_addr is not None

        if context is None:
            context = zmq.Context(1)
        socket = context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect(self._exit_addr)
        socket.send(_LogbookThread._EXIT_CODE)
        time.sleep(0.5) # Acceptable exit time
        assert not self.isAlive()
        socket.close()


class TestLogbookReplication(unittest.TestCase):
    def setUp(self):
        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = _LogbookThread(args, 'tcp://127.0.0.1:8090')
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
        self.logbook.stop(self.context)
        self.assertFalse(self.logbook.isAlive(),
                         "Logbook should not have been running. It was.")

        self.context.term()


class TestLogbookQuerying(unittest.TestCase):
    def setUp(self):
        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--query-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = _LogbookThread(args, 'tcp://127.0.0.1:8090')
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

        self.assertTrue(self.logbook.isAlive(),
                        "Did logbook crash? Not running.")
        self.logbook.stop(self.context)
        self.assertFalse(self.logbook.isAlive(),
                         "Logbook should not have been running. It was.")

        self.context.term()


class TestKeyValuePersister(unittest.TestCase):
    keyvals = {
        'key1': 'val1',
        'key2': 'value number two',
        'key3': 'val3',
    }

    def setUp(self):
        namedfile = tempfile.NamedTemporaryFile(delete=False)
        self.keyvalfile = namedfile.name
        keyvalpersister = self._open_persister()

        self.namedfile = namedfile
        self.keyvalpersister = keyvalpersister

    def _open_persister(self):
        return gtdoit.logbook.KeyValuePersister(self.keyvalfile, " ")

    def tearDown(self):
        if self.keyvalpersister:
            self.keyvalpersister.close()
            self.keyvalpersister = None
        self.namedfile.close()
        self.keyvalfile = None
        self.namedfile = None

    def _write_keyvals(self):
        for key, val in self.keyvals.iteritems():
            self.keyvalpersister[key] = val

    def _assertValuesWereWritten(self):
        for key, val in self.keyvals.iteritems():
            self.assertTrue(key in self.keyvalpersister)
            self.assertEqual(self.keyvalpersister[key], val)
        self.assertEqual(len(self.keyvalpersister), len(self.keyvals))

    def testAppending(self):
        self._write_keyvals()
        self._assertValuesWereWritten()

    def _assert_delimieter_key_exception(self):
        faulty_keys = ["a key", "key ", " key"]
        for key in faulty_keys:
            self.assertRaises(gtdoit.logbook.KeyValuePersister.InsertError,
                              lambda x,y: self.keyvalpersister.__setitem__(x,y),
                              key, "5")

    def testAppendingKeyContainingDelimiter(self):
        self._assert_delimieter_key_exception()
        self.assertEqual(len(self.keyvalpersister), 0)

    def testWritingAfterInsertError(self):
        self.testAppendingKeyContainingDelimiter()
        self._write_keyvals()
        self._assert_delimieter_key_exception()
        self.assertEqual(len(self.keyvalpersister), 3)
        self._assertValuesWereWritten()

    def testReopen(self):
        self._write_keyvals()
        self._assertValuesWereWritten()
        self.keyvalpersister.close()
        self.keyvalpersister = self._open_persister()
        self._assertValuesWereWritten()

    def testIter(self):
        self._write_keyvals()
        vals = iter(self.keyvalpersister)
        self.assertEqual(set(vals), set(self.keyvals))

    def testDuplicateKeyCheck(self):
        self._write_keyvals()
        self.assertRaises(gtdoit.logbook.KeyValuePersister.InsertError,
                          self.keyvalpersister.__setitem__,
                          self.keyvals.keys()[0], "56")

    def testDelItem(self):
        """Test __delitem__ behaviour.

        __delitem__ is not really used, but we want to keep 100% coverage,
        so...
        """
        self.assertRaises(NotImplementedError, self.keyvalpersister.__delitem__,
                          self.keyvals.keys()[0])

    def testFileOutput(self):
        """Making sure we are writing in md5sum format."""
        self._write_keyvals()

        self.keyvalpersister.close()
        self.keyvalpersister = None # Needed so tearDown doesn't close

        with open(self.keyvalfile) as f:
            content = f.read()
            actual_lines = content.splitlines()
            expected_lines = ["%s %s" % (k,v)
                              for k,v in self.keyvals.iteritems()]
        self.assertEquals(actual_lines, expected_lines)

