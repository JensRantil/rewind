from __future__ import print_function
import contextlib
import itertools
import random
import hashlib
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid
import os
import re

import mock
import zmq

import rewind.communicators as communicators
import rewind.logbook as logbook


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
        print("Populating with {0} events...".format(N))
        self.keys = ["{0}".format(i) for i in range(N)]
        self.vals = ["{0}".format(i + 30).encode() for i in range(N)]
        self.items = list(zip(self.keys, self.vals))
        for key, val in zip(self.keys, self.vals):
            self.store.add_event(key, val)

    def testQueryingAll(self):
        result = self.store.get_events()
        self.assertEqual(list(result), self.items)

    def testQueryAfter(self):
        result = self.store.get_events(from_=self.keys[0])
        self.assertEqual(list(result), self.items[1:])
        result = self.store.get_events(from_=self.keys[1])
        self.assertEqual(list(result), self.items[2:])

    def testQueryBefore(self):
        result = self.store.get_events(to=self.keys[-1])
        self.assertEqual(list(result), self.items)
        result = self.store.get_events(to=self.keys[-2])
        self.assertEqual(list(result), self.items[:-1])

    def testQueryBetween(self):
        result = self.store.get_events(from_=self.keys[1], to=self.keys[-2])
        self.assertEqual(list(result), self.items[2:-1])

    def testKeyExists(self):
        for key in self.keys:
            self.assertTrue(self.store.key_exists(key),
                            "Key did not exist: {0}".format(key))


class TestEventStore(unittest.TestCase):
    """Tests the class `EventStore`."""
    def testStubs(self):
        """Makes sure `EventStore` behaves the way we expect."""
        estore = logbook.EventStore()
        self.assertRaises(NotImplementedError, estore.add_event, b"key",
                          b"event")
        self.assertRaises(NotImplementedError, estore.get_events)
        self.assertRaises(NotImplementedError, estore.get_events, b"from")
        self.assertRaises(NotImplementedError, estore.get_events, b"from",
                          b"to")
        self.assertRaises(NotImplementedError, estore.key_exists, b"key")
        estore.close()  # Should not throw anything


class TestSyncedRotationEventStores(unittest.TestCase, _TestEventStore):
    """Test `SyncedRotationEventStores`."""

    # Number of events per batch
    EVS_PER_BATCH = 5

    def setUp(self):
        basedir = tempfile.mkdtemp()
        rotated_estore_params = [
            {
                'dirpath': os.path.join(basedir, 'db'),
                'prefix': 'logdb',
            },
            {
                'dirpath': os.path.join(basedir, 'log'),
                'prefix': 'appendlog',
            },
        ]

        self.rotated_estore_params = rotated_estore_params
        self.basedir = basedir

        self._openStore()
        self._populate_store()

    def _init_rotated_stores(self):
        rotated_stores = []
        mocked_factories = []

        for params in self.rotated_estore_params:
            if params['prefix'] == 'logdb':
                factory = logbook._SQLiteEventStore
            elif params['prefix'] == 'appendlog':
                factory = logbook._LogEventStore
            else:
                self.fail('Unrecognized prefix.')
            factory = mock.Mock(wraps=factory)
            mocked_factories.append(factory)

            with mock.patch('os.mkdir', side_effect=os.mkdir) as mkdir_mock:
                rotated_store = logbook.RotatedEventStore(factory,
                                                          **params)
                mkdir_mock.assert_called_once(params['dirpath'])

            fname_absolute = os.path.join(params['dirpath'],
                                          "{0}.0".format(params['prefix']))

            # If it wasn't for the fact that this class function was called
            # from testReopening, we would be able to also assert that the
            # factory was called with correct parameters.
            self.assertEqual(factory.call_count, 1)

            rotated_stores.append(rotated_store)

        self.rotated_stores = rotated_stores
        self.mocked_factories = mocked_factories

    def _openStore(self):
        self._init_rotated_stores()

        evs_per_batch = TestSyncedRotationEventStores.EVS_PER_BATCH
        store = logbook.SyncedRotationEventStores(evs_per_batch)
        for rotated_store in self.rotated_stores:
            store.add_rotated_store(rotated_store)
        self.store = store

    def tearDown(self):
        if self.store is not None:
            # Only close if no other test has already closed it and assigned it
            # None.
            self.store.close()

            # Asserting every single EventStore instantiated has had close()
            # called upon it.
            for mocked_factory in self.mocked_factories:
                for call in mocked_factory.mock_calls:
                    call.return_value.close.assert_called_once_with()

        self.assertTrue(os.path.exists(self.basedir))
        shutil.rmtree(self.basedir)
        self.assertFalse(os.path.exists(self.basedir))

    def testReopening(self):
        events_before_reload = self.store.get_events()
        self.store.close()
        self._openStore()
        events_after_reload = self.store.get_events()
        self.assertEqual(list(events_before_reload), list(events_after_reload))

    def testKeyExists(self):
        evs_per_batch = TestSyncedRotationEventStores.EVS_PER_BATCH
        nkeys_in_last_batch = len(self.keys) % evs_per_batch
        if nkeys_in_last_batch > 0:
            # No reasons to test if there were no events written to this batch
            keys_in_last_batch = self.keys[-nkeys_in_last_batch:]
            for key in keys_in_last_batch:
                self.assertTrue(self.store.key_exists(key),
                                "Key did not exist: {0}".format(key))

    def _check_md5_is_correct(self, dirpath):
        print("Directory:", dirpath)
        md5filename = os.path.join(dirpath, 'checksums.md5')
        self.assertTrue(os.path.exists(md5filename))

        checksums = logbook.KeyValuePersister(md5filename)
        files = [fname for fname in os.listdir(dirpath) if
                 fname != 'checksums.md5']
        self.assertEqual(set(files), set(checksums.keys()))

        for fname, checksum in checksums.items():
            hasher = hashlib.md5()
            abspath = os.path.join(dirpath, fname)
            with open(abspath) as f:
                logbook._hashfile(f, hasher)
            self.assertEqual(hasher.hexdigest(), checksum)

    def testMD5WasWritten(self):
        """Asserting MD5 files were written."""
        self.store.close()
        self.store = None
        for param in self.rotated_estore_params:
            self._check_md5_is_correct(param['dirpath'])


class TestRotatedEventStorage(unittest.TestCase, _TestEventStore):
    def setUp(self):
        """Setup method before each test.

        TODO: Use loops instead of suffixed variables.
        """
        N = 20

        mstore1 = logbook.InMemoryEventStore()
        mstore1.close = mock.MagicMock()  # Needed for assertions
        keys1 = ["{0}".format(i) for i in range(N)]
        vals1 = ["{0}".format(i + 30).encode() for i in range(N)]
        for key, val in zip(keys1, vals1):
            mstore1.add_event(key, val)

        mstore2 = logbook.InMemoryEventStore()
        mstore2.close = mock.MagicMock()  # Needed for assertions
        keys2 = ["{0}".format(i + N) for i in range(N)]
        vals2 = ["{0}".format(i + 30 + N).encode() for i in range(N)]
        for key, val in zip(keys2, vals2):
            mstore2.add_event(key, val)

        mstore3 = logbook.InMemoryEventStore()
        mstore3.close = mock.MagicMock()  # Needed for assertions
        keys3 = ['one', 'two', 'three']
        vals3 = [b'four', b'five', b'six']
        for key, val in zip(keys3, vals3):
            mstore3.add_event(key, val)

        mstore4 = logbook.InMemoryEventStore()

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
            store = logbook.RotatedEventStore(estore_factory,
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
        self.items = list(zip(self.keys, self.vals))
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

        self.assertFalse(self.store.key_exists(b'mykey'))
        self.store.add_event(b'mykey', 'myvalue')
        self.assertTrue(self.store.key_exists(b'mykey'),
                        "The event was expected to have been written.")
        self.assertTrue(self.mstore4.key_exists(b'mykey'),
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
        self.tempfile.close()  # We are not to modify it directly
        self.store = logbook._LogEventStore(self.tempfile.name)

        self._populate_store()

    def testReopenWithClose(self):
        self.store.close()
        self.store = logbook._LogEventStore(self.tempfile.name)
        self.assertEqual(len(self.keys), len(self.vals),
                         "Keys and vals did not match in number.")
        self.assertEqual(len(self.store.get_events(),), len(self.keys))

    def testCorruptionCheckOnOpen(self):
        """Asserting we identify corrupt `LogEventStore` files."""
        self.store.close()
        with open(self.tempfile.name, 'wb') as f:
            f.write(b"Random data %%%!!!??")
        self.assertRaises(logbook.LogBookCorruptionError,
                          logbook._LogEventStore,
                          self.tempfile.name)

    def tearDown(self):
        self.store.close()
        os.remove(self.tempfile.name)


class TestSQLiteEventStore(unittest.TestCase, _TestEventStore):
    """Test `_SQLiteEventStore`."""
    def setUp(self):
        self.tempfile = tempfile.NamedTemporaryFile(prefix='test_logbook',
                                                    suffix='sqlite_evstore',
                                                    delete=False)
        self.tempfile.close()  # We are not to modify it directly
        self.store = logbook._SQLiteEventStore(self.tempfile.name)

        self._populate_store()

    def testCount(self):
        self.assertEqual(len(self.keys), len(self.vals),
                         "Keys and vals did not match in number.")
        self.assertTrue(self.store.count() == len(self.keys),
                        "Count was incorrect.")

    def testReopenWithClose(self):
        self.store.close()
        self.store = logbook._SQLiteEventStore(self.tempfile.name)

        # testCount does exactly the test we want to do. Reusing it.
        self.testCount()

    def testCorruptionCheckOnOpen(self):
        """Asserting we identify corrupt `_SQLiteEventStore` files."""
        self.store.close()
        with open(self.tempfile.name, 'wb') as f:
            f.write(b"Random data %%%!!!??")
        self.assertRaises(logbook.LogBookCorruptionError,
                          logbook._SQLiteEventStore,
                          self.tempfile.name)

    def tearDown(self):
        self.store.close()
        os.remove(self.tempfile.name)


class TestInMemoryEventStore(unittest.TestCase, _TestEventStore):
    """Test `InMemoryEventStore`."""
    def setUp(self):
        self.store = logbook.InMemoryEventStore()
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
            logbook = _LogbookThread(['--streaming-bind-endpoint',
                                      'tcp://hello'])
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
        print("Using datapath:", datapath)

        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091',
                '--datadir', datapath]
        print(" ".join(args))
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
    _EXIT_CODE = b'EXIT'

    def __init__(self, cmdline_args, exit_addr=None):
        """Constructor.

        Parameters:
        cmdline_args -- command line arguments used to execute the logbook.
        exit_addr    -- the ZeroMQ address used to send the exit message to.
        """
        thread = self

        assert '--exit-codeword' not in cmdline_args, \
               "'--exit-codeword' is added by _LogbookThread. Not elsewhere"
        cmdline_args = (['--exit-codeword',
                         _LogbookThread._EXIT_CODE.decode()] +
                        cmdline_args)

        def exitcode_runner(*args, **kwargs):
            try:
                thread.exit_code = logbook.main(*args, **kwargs)
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
        time.sleep(0.5)  # Acceptable exit time
        assert not self.isAlive()
        socket.close()


class TestLogbookReplication(unittest.TestCase):

    UUID_REGEXP = ("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                   "[0-9a-f]{12}")

    def setUp(self):
        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.logbook = _LogbookThread(args, 'tcp://127.0.0.1:8090')
        self.logbook.start()

        self.context = zmq.Context(3)

        self.transmitter = self.context.socket(zmq.PUSH)
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.setsockopt(zmq.SUBSCRIBE, b'')

        self.transmitter.connect('tcp://127.0.0.1:8090')
        self.receiver.connect('tcp://127.0.0.1:8091')

        # Time it takes to connect. This is particularly important so that the
        # receiver does not just receive the tail of the stream.
        time.sleep(0.5)

        # Making sure context.term() does not time out
        # Could be removed if this test works as expected
        self.transmitter.setsockopt(zmq.LINGER, 1000)

    def testBasicEventProxying(self):
        eventid = b"abc12332fffgdgaab134432423"
        eventstring = b"THIS IS AN EVENT"

        self.transmitter.send(eventstring)

        received_id = self.receiver.recv()
        self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
        received_string = self.receiver.recv()
        self.assertFalse(self.receiver.getsockopt(zmq.RCVMORE))

        self.assertIsNotNone(re.match(self.UUID_REGEXP, received_id))
        self.assertEqual(received_string, eventstring)

    def testProxyingABunchOfEvents(self):
        """Tests that a bunch of incoming messages processed correctly.

        That is, they are all being proxied and in order.
        """
        NMESSAGES = 200
        messages = []
        for id in range(NMESSAGES):
            eventstring = "THIS IS EVENT NUMBER {0}".format(id).encode()
            messages.append(eventstring)

        # Sending
        for msg in messages:
            self.transmitter.send(msg)

        # Receiving and asserting correct messages
        eventids = []
        for msg in messages:
            received_id = self.receiver.recv()
            self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
            received_string = self.receiver.recv()
            self.assertFalse(self.receiver.getsockopt(zmq.RCVMORE))

            self.assertIsNotNone(re.match(self.UUID_REGEXP, received_id))
            eventids.append(received_id)
            self.assertEqual(received_string, msg)

        self.assertEqual(len(set(eventids)), len(eventids),
                         "Found duplicate event id!")

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

        ids = [uuid.uuid1().hex for i in range(200)]
        self.assertEqual(len(ids), len(set(ids)), 'There were duplicate IDs.'
                         ' Maybe the UUID1 algorithm is flawed?')
        users = [uuid.uuid1().hex for i in range(30)]
        self.assertEqual(len(users), len(set(users)),
                         'There were duplicate users.'
                         ' Maybe the UUID1 algorithm is flawed?')

        self.sent = []
        for id in ids:
            eventstr = "Event with id '{0}'".format(id).encode()
            transmitter.send(eventstr)
            self.sent.append(eventstr)
        transmitter.close()

    def testSyncAllPastEvents(self):
        time.sleep(0.5)  # Max time to persist the messages
        allevents = [event[1] for event in self.querier.query()]
        self.assertEqual(allevents, self.sent)

        self.assertEqual(allevents, self.sent, "Elements don't match.")

    def testSyncEventsSince(self):
        time.sleep(0.5)  # Max time to persist the messages
        allevents = [event for event in self.querier.query()]
        from_ = allevents[3][0]
        events = [event[1] for event in self.querier.query(from_=from_)]
        self.assertEqual([event[1] for event in allevents[4:]], events)

    def testSyncEventsBefore(self):
        time.sleep(0.5)  # Max time to persist the messages
        allevents = [event for event in self.querier.query()]
        to = allevents[-3][0]
        events = [event[1] for event in self.querier.query(to=to)]
        self.assertEqual([event[1] for event in allevents[:-2]], events)

    def testSyncEventsBetween(self):
        time.sleep(0.5)  # Max time to persist the messages
        allevents = [event for event in self.querier.query()]
        from_ = allevents[3][0]
        to = allevents[-3][0]
        events = [event[1] for event in self.querier.query(from_=from_, to=to)]
        self.assertEqual([event[1] for event in allevents[4:-2]], events)

    def testSyncNontExistentEvent(self):
        result = self.querier.query(from_="non-exist")
        self.assertRaises(communicators.EventQuerier.QueryException,
                          list, result)

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
        return logbook.KeyValuePersister(self.keyvalfile)

    def tearDown(self):
        if self.keyvalpersister:
            self.keyvalpersister.close()
            self.keyvalpersister = None
        self.namedfile.close()
        self.keyvalfile = None
        self.namedfile = None

    def _write_keyvals(self):
        for key, val in self.keyvals.items():
            self.keyvalpersister[key] = val

    def _assertValuesWereWritten(self):
        for key, val in self.keyvals.items():
            self.assertTrue(key in self.keyvalpersister)
            self.assertEqual(self.keyvalpersister[key], val)
        self.assertEqual(len(self.keyvalpersister), len(self.keyvals))

    def testAppending(self):
        self._write_keyvals()
        self._assertValuesWereWritten()

    def _assert_delimieter_key_exception(self):
        faulty_kvs = [("a key", "value"), ("key ", "value"),
                      (" key", "value"), ("multiline\nkey", "value"),
                      ("key", "multiline\nvalue")]
        for key, val in faulty_kvs:
            setter = lambda: self.keyvalpersister.__setitem__(key, val)
            self.assertRaises(logbook.KeyValuePersister.InsertError, setter)

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
        for i in range(3):
            self.keyvalpersister.close()
            self.keyvalpersister = self._open_persister()
            self._assertValuesWereWritten()

    def testIter(self):
        self._write_keyvals()
        vals = iter(self.keyvalpersister)
        self.assertEqual(set(vals), set(self.keyvals))

    def testChangingValue(self):
        self._write_keyvals()

        # Changing value of the first key
        first_key = next(iter(self.keyvals.keys()))
        new_value = "56"
        self.assertNotEqual(self.keyvalpersister[first_key], new_value)
        self.keyvalpersister[first_key] = new_value

        self.assertEqual(self.keyvalpersister[first_key], new_value)

    def testDelItem(self):
        """Test __delitem__ behaviour.

        __delitem__ is not really used, but we want to keep 100% coverage,
        so...
        """
        self.assertRaises(NotImplementedError,
                          self.keyvalpersister.__delitem__,
                          next(iter(self.keyvals.keys())))

    def testFileOutput(self):
        """Making sure we are writing in md5sum format."""
        self._write_keyvals()

        self.keyvalpersister.close()
        self.keyvalpersister = None  # Needed so tearDown doesn't close

        with open(self.keyvalfile, 'r') as f:
            content = f.read()
            actual_lines = content.splitlines()
            expected_lines = ["{0} {1}".format(k, v)
                              for k, v in self.keyvals.items()]
        self.assertEquals(actual_lines, expected_lines)

    def testOpeningNonExistingFile(self):
        randomfile = tempfile.NamedTemporaryFile()
        randomfile.close()
        self.assertFalse(os.path.exists(randomfile.name),
                         "Expected file to not exist.")
        logbook.KeyValuePersister(randomfile.name)
