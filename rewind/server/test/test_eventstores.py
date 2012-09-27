# Rewind is an event store server written in Python that talks ZeroMQ.
# Copyright (C) 2012  Jens Rantil
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Tests of event stores."""
from __future__ import print_function
import contextlib
import hashlib
import itertools
import os
import random
import shutil
import tempfile
import unittest

import mock

import rewind.server.eventstores as eventstores
import rewind.server.rewind as rewind


class TestKeyValuePersister(unittest.TestCase):

    """Test `_KeyValuePersister`."""

    # Test data
    keyvals = {
        'key1': 'val1',
        'key2': 'value number two',
        'key3': 'val3',
    }

    def setUp(self):
        """Set up a prepopulated `_KeyValuePersister`."""
        namedfile = tempfile.NamedTemporaryFile(delete=False)
        self.keyvalfile = namedfile.name
        keyvalpersister = self._open_persister()

        self.namedfile = namedfile
        self.keyvalpersister = keyvalpersister

    def _open_persister(self):
        """Return a newly opened prepopulated `_KeyValuePersister`."""
        return eventstores._KeyValuePersister(self.keyvalfile)

    def tearDown(self):
        """Close the opened `_KeyValuePersister`, if needed."""
        if self.keyvalpersister:
            self.keyvalpersister.close()
            self.keyvalpersister = None
        self.namedfile.close()
        os.unlink(self.keyvalfile)
        self.keyvalfile = None
        self.namedfile = None

    def _write_keyvals(self):
        """Prepopulate the already opened `_KeyValuePersister`."""
        for key, val in self.keyvals.items():
            self.keyvalpersister[key] = val

    def _assertValuesWereWritten(self):
        """Assert the prepopulated values were written to disk."""
        for key, val in self.keyvals.items():
            self.assertTrue(key in self.keyvalpersister)
            self.assertEqual(self.keyvalpersister[key], val)
        self.assertEqual(len(self.keyvalpersister), len(self.keyvals))

    def testAppending(self):
        """Test appending new values to the test persister."""
        self._write_keyvals()
        self._assertValuesWereWritten()

    def _assert_delimieter_key_exception(self):
        """Make sure we throw exceptions on malformated keys and values.

        TODO: Correct incorrect spelling of this function.

        """
        faulty_kvs = [("a key", "value"), ("key ", "value"),
                      (" key", "value"), ("multiline\nkey", "value"),
                      ("key", "multiline\nvalue")]
        for key, val in faulty_kvs:
            setter = lambda: self.keyvalpersister.__setitem__(key, val)
            self.assertRaises(eventstores._KeyValuePersister.InsertError,
                              setter)

    def testAppendingKeyContainingDelimiter(self):
        """Make sure we throw exceptions on malformated keys and values."""
        self._assert_delimieter_key_exception()
        self.assertEqual(len(self.keyvalpersister), 0)

    def testWritingAfterInsertError(self):
        """Make sure we can write correct k/vs after an incorrect one."""
        self.testAppendingKeyContainingDelimiter()
        self._write_keyvals()
        self._assert_delimieter_key_exception()
        self.assertEqual(len(self.keyvalpersister), 3)
        self._assertValuesWereWritten()

    def testReopen(self):
        """Test closing en reopening a `_KeyValuePersister`."""
        self._write_keyvals()
        self._assertValuesWereWritten()
        for i in range(3):
            self.keyvalpersister.close()
            self.keyvalpersister = self._open_persister()
            self._assertValuesWereWritten()

    def testIter(self):
        """Test iterating over key-values in a `_KeyValuePersister`."""
        self._write_keyvals()
        vals = iter(self.keyvalpersister)
        self.assertEqual(set(vals), set(self.keyvals))

    def testChangingValue(self):
        """Test changing a value in `_KeyValuePersister`."""
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
        """Test we don't throw exception when opening non-existing file."""
        randomfile = tempfile.NamedTemporaryFile()
        randomfile.close()
        self.assertFalse(os.path.exists(randomfile.name),
                         "Expected file to not exist.")
        eventstores._KeyValuePersister(randomfile.name)

    def testErrorWriting(self):
        """Test rewritten values with errors are not changed."""
        self._write_keyvals()
        self._assertValuesWereWritten()

        # Poor man's flush
        self.keyvalpersister.close()
        self.keyvalpersister = self._open_persister()
        os.chmod(self.keyvalfile, 0o400)

        testkey = next(iter(self.keyvals))

        def modify_existing_key():
            self.keyvalpersister[testkey] = "45934857984"
        self.assertRaises(IOError, modify_existing_key)

        # Assert we did not change anything
        self._assertValuesWereWritten()

        # Important - otherwise tearDown will fail because we did not have
        # permissions to reopen the persister since we changed the file write
        # permissions.
        self.keyvalpersister = None


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
        """Test query for all events."""
        result = self.store.get_events()
        self.assertEqual(list(result), self.items)

    def testQueryAfter(self):
        """Test to query all events after a certain time."""
        result = self.store.get_events(from_=self.keys[0])
        self.assertEqual(list(result), self.items[1:])
        result = self.store.get_events(from_=self.keys[1])
        self.assertEqual(list(result), self.items[2:])

    def testQueryBefore(self):
        """Test to query all events before a certain time."""
        result = self.store.get_events(to=self.keys[-1])
        self.assertEqual(list(result), self.items)
        result = self.store.get_events(to=self.keys[-2])
        self.assertEqual(list(result), self.items[:-1])

    def testQueryBetween(self):
        """Test to query events between to times."""
        result = self.store.get_events(from_=self.keys[1], to=self.keys[-2])
        self.assertEqual(list(result), self.items[2:-1])

    def testKeyExists(self):
        """Test `EventStore.key_exists(...)` behaviour."""
        for key in self.keys:
            self.assertTrue(self.store.key_exists(key),
                            "Key did not exist: {0}".format(key))


class TestEventStore(unittest.TestCase):

    """Tests the class `EventStore`."""

    def testStubs(self):
        """Make sure `EventStore` behaves the way we expect."""
        estore = eventstores.EventStore()
        estore = eventstores.EventStore()
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
        """Prepare each test."""
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
                factory = eventstores.SQLiteEventStore
            elif params['prefix'] == 'appendlog':
                factory = eventstores.LogEventStore
            else:
                self.fail('Unrecognized prefix.')
            factory = mock.Mock(wraps=factory)
            mocked_factories.append(factory)

            with mock.patch('os.mkdir', side_effect=os.mkdir) as mkdir_mock:
                rotated_store = eventstores.RotatedEventStore(factory,
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
        store = eventstores.SyncedRotationEventStores(evs_per_batch)
        for rotated_store in self.rotated_stores:
            store.add_rotated_store(rotated_store)
        self.store = store

    def tearDown(self):
        """Close temp store if necessary and assert it was closed correctly.

        Also making sure to remove the temporary store from disk.

        """
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
        """Test closing and reopening `RotatedEventStore`."""
        events_before_reload = self.store.get_events()
        self.store.close()
        self._openStore()
        events_after_reload = self.store.get_events()
        self.assertEqual(list(events_before_reload), list(events_after_reload))

    def testKeyExists(self):
        """Test `RotatedEventStore.key_exists(...)`."""
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

        checksums = eventstores._KeyValuePersister(md5filename)
        files = [fname for fname in os.listdir(dirpath) if
                 fname != 'checksums.md5']
        self.assertEqual(set(files), set(checksums.keys()))

        for fname, checksum in checksums.items():
            hasher = hashlib.md5()
            abspath = os.path.join(dirpath, fname)
            with open(abspath, 'rb') as f:
                eventstores._hashfile(f, hasher)
            self.assertEqual(hasher.hexdigest(), checksum)

    def testMD5WasWritten(self):
        """Asserting MD5 files were written."""
        self.store.close()
        self.store = None
        for param in self.rotated_estore_params:
            self._check_md5_is_correct(param['dirpath'])


class TestRotatedEventStore(unittest.TestCase, _TestEventStore):

    """Test `RotatedEventStore`."""

    def setUp(self):
        """Setup method before each test.

        TODO: Use loops instead of suffixed variables.

        Returns nothing.

        """
        N = 20

        mstore1 = eventstores.InMemoryEventStore()
        mstore1.close = mock.MagicMock()  # Needed for assertions
        keys1 = ["{0}".format(i) for i in range(N)]
        vals1 = ["{0}".format(i + 30).encode() for i in range(N)]
        for key, val in zip(keys1, vals1):
            mstore1.add_event(key, val)

        mstore2 = eventstores.InMemoryEventStore()
        mstore2.close = mock.MagicMock()  # Needed for assertions
        keys2 = ["{0}".format(i + N) for i in range(N)]
        vals2 = ["{0}".format(i + 30 + N).encode() for i in range(N)]
        for key, val in zip(keys2, vals2):
            mstore2.add_event(key, val)

        mstore3 = eventstores.InMemoryEventStore()
        mstore3.close = mock.MagicMock()  # Needed for assertions
        keys3 = ['one', 'two', 'three']
        vals3 = [b'four', b'five', b'six']
        for key, val in zip(keys3, vals3):
            mstore3.add_event(key, val)

        mstore4 = eventstores.InMemoryEventStore()

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
            store = eventstores.RotatedEventStore(estore_factory,
                                                  '/random_dir', 'eventdb')
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
        """Test that rotation works."""
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

    """Test `_LogEventStore`."""

    def setUp(self):
        """Prepare a temporary test `_LogEventStore`."""
        self.tempfile = tempfile.NamedTemporaryFile(prefix='test_rewind',
                                                    suffix='.log',
                                                    delete=False)
        self.tempfile.close()  # We are not to modify it directly
        self.store = eventstores.LogEventStore(self.tempfile.name)

        self._populate_store()

    def testReopenWithClose(self):
        """Test closing and reopening a `_LogEventStore`."""
        self.store.close()
        self.store = eventstores.LogEventStore(self.tempfile.name)
        self.assertEqual(len(self.keys), len(self.vals),
                         "Keys and vals did not match in number.")
        self.assertEqual(len(self.store.get_events(),), len(self.keys))

    def testCorruptionCheckOnOpen(self):
        """Assert we identify corrupt `_LogEventStore` files."""
        self.store.close()
        with open(self.tempfile.name, 'wb') as f:
            f.write(b"Random data %%%!!!??")
        self.assertRaises(eventstores.CorruptionError,
                          eventstores.LogEventStore,
                          self.tempfile.name)

    def tearDown(self):
        """Close and remove the temporary store."""
        self.store.close()
        os.remove(self.tempfile.name)


class TestSQLiteEventStore(unittest.TestCase, _TestEventStore):

    """Test `SQLiteEventStore`."""

    def setUp(self):
        """Create and populate a temporary `_SQLiteEventStore`."""
        self.tempfile = tempfile.NamedTemporaryFile(prefix='test_rewind',
                                                    suffix='sqlite_evstore',
                                                    delete=False)
        self.tempfile.close()  # We are not to modify it directly
        self.store = eventstores.SQLiteEventStore(self.tempfile.name)

        self._populate_store()

    def testCount(self):
        """Test counting the number of events added."""
        self.assertEqual(len(self.keys), len(self.vals),
                         "Keys and vals did not match in number.")
        self.assertTrue(self.store.count() == len(self.keys),
                        "Count was incorrect.")

    def testReopenWithClose(self):
        """Test closing and reopening a store."""
        self.store.close()
        self.store = eventstores.SQLiteEventStore(self.tempfile.name)

        # testCount does exactly the test we want to do. Reusing it.
        self.testCount()

    def testCorruptionCheckOnOpen(self):
        """Asserting we identify corrupt `SQLiteEventStore` files."""
        self.store.close()
        with open(self.tempfile.name, 'wb') as f:
            f.write(b"Random data %%%!!!??")
        self.assertRaises(eventstores.CorruptionError,
                          eventstores.SQLiteEventStore,
                          self.tempfile.name)

    def tearDown(self):
        """Close and remove temporary store used by tests."""
        self.store.close()
        os.remove(self.tempfile.name)


class TestInMemoryEventStore(unittest.TestCase, _TestEventStore):

    """Test `InMemoryEventStore`."""

    def setUp(self):
        """Initialize an `InMemoryEventStore` used for testing."""
        self.store = eventstores.InMemoryEventStore()
        self._populate_store()
