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

"""Test overall Rewind execution."""
from __future__ import print_function
try:
    # Python < 3
    import ConfigParser as configparser
except ImportError:
    # Python >= 3
    import configparser
import contextlib
import itertools
import os.path
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid

import mock
import zmq

import rewind.server.eventstores as eventstores
import rewind.server.rewind as rewind


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

    """Tests various command line arguments for `rewind`."""

    def setUp(self):
        """Prepare each command line execution test."""
        # See tearDown() why this one is defined
        self.rewind = None

    def tearDown(self):
        """Making sure rewind is closed after each test."""
        if self.rewind and self.rewind.isAlive():
            # Making sure to close rewind if it has been defined
            self.rewind.stop()
            self.rewind = None

    def testAtLeastOneEndpointRequired(self):
        """Asserting we fail if no endpoint is defined."""
        with _direct_stderr_to_stdout():
            rewind = _RewindRunnerThread([])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 2)

    def testOnlyStreamingEndpointFails(self):
        """Assert Rewind won't start with only streaming endpoint defined."""
        with _direct_stderr_to_stdout():
            rewind = _RewindRunnerThread(['--streaming-bind-endpoint',
                                          'tcp://hello'])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 2)

    def testHelp(self):
        """Testing commend line `--help` listing works."""
        with _direct_stderr_to_stdout():
            rewind = _RewindRunnerThread(['--help'])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 0)

    def testStartingWithPersistence(self):
        """Testing starting and stopping from command line."""
        datapath = tempfile.mkdtemp()
        print("Using datapath:", datapath)

        tempconfig = tempfile.NamedTemporaryFile()
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")
        config.set("estoresection", "class",
                   "rewind.server.eventstores.SQLiteEventStore")
        config.set("estoresection", "path", os.path.join(datapath,
                                                         "db.sqlite"))
        config.write(tempconfig)
        tempconfig.flush()

        args = ['--query-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091',
                '--configfile', tempconfig.name]
        print(" ".join(args))
        self.rewind = _RewindRunnerThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

        time.sleep(3)
        self.assertTrue(self.rewind.isAlive(),
                        "Rewind was not running for more than 3 seconds")

        # Not removing this in tearDown for two reasons:
        # 1. Datapath is not created in setUp()
        # 2. If this test fails, we will keep the datapath that was created.
        shutil.rmtree(datapath)

    def testIncorrectConfiguration(self):
        """Testing starting using incorrect configuration."""
        datapath = tempfile.mkdtemp()
        print("Using datapath:", datapath)

        tempconfig = tempfile.NamedTemporaryFile()
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")
        config.set("estoresection", "class",
                   "rewind.server.eventstores.SQLiteEventStore")
        # Deliberately leaving 'path' here to simulate a configuration error
        config.write(tempconfig)
        tempconfig.flush()

        args = ['--query-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091',
                '--configfile', tempconfig.name]
        print(" ".join(args))
        self.rewind = _RewindRunnerThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

        time.sleep(1)
        self.assertFalse(self.rewind.isAlive(),
                         "Rewind was running for more than 1 second")
        self.assertEquals(os.listdir(datapath), [],
                          "Expected no file to have been created.")

        # Not removing this in tearDown for two reasons:
        # 1. Datapath is not created in setUp()
        # 2. If this test fails, we will keep the datapath that was created.
        shutil.rmtree(datapath)


class TestInitialization(unittest.TestCase):

    """Test `rewind.construct_eventstore(...)` behaviour."""

    def testInMemoryFallback(self):
        """Test `construct_eventstore(...)` defaults to in-memory estore."""
        estore = rewind.construct_eventstore(None, [])
        self.assertIsInstance(estore, eventstores.InMemoryEventStore)

    def testStringRepresentationOfConfigurationError(self):
        """Test _ConfigurationError.__str__ behaviour.

        It's not crucial behaviour, but always worth the coverage.

        """
        err = rewind._ConfigurationError("Bad string")
        self.assertEquals(str(err), "'Bad string'")

    def testMissingDefaultSection(self):
        """Test `construct_eventstore(...)` bails on no default section."""
        config = configparser.ConfigParser()
        self.assertRaises(rewind._ConfigurationError,
                          rewind.construct_eventstore, config, [])

    def testMissingConfigEventStoreSection(self):
        """Test `construct_eventstore(...)` bails on missing class section."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "nonexistsection")
        self.assertRaises(rewind._ConfigurationError,
                          rewind.construct_eventstore, config, [])

    def testMissingArgumentEventStoreSection(self):
        """Test `construct_eventstore(...)` bails on missing arg section."""
        config = configparser.ConfigParser()
        self.assertRaises(rewind._ConfigurationError,
                          rewind.construct_eventstore, config, [],
                          "nonexistsection")

    def testMissingEventStoreClass(self):
        """Test `construct_eventstore(...)` bails on missing class path."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")
        self.assertRaises(rewind._ConfigurationError,
                          rewind.construct_eventstore, config, [])

    def testCreatingInMemoryStoreUsingConfig(self):
        """Full test of `construct_eventstore(...)`."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")

        config.set("estoresection", "class",
                   "rewind.server.eventstores.InMemoryEventStore")

        estore = rewind.construct_eventstore(config, [])

        self.assertIsInstance(estore, eventstores.InMemoryEventStore)

    def testCreatingInMemoryStoreUsingConfigWithGivenSection(self):
        """Test full test of `construct_eventstore(...)` given a section."""
        config = configparser.ConfigParser()
        config.add_section("estoresection")
        # The 'general' section need not to be defined here since a section is
        # given to `construct_eventstore(...)` below.

        config.set("estoresection", "class",
                   "rewind.server.eventstores.InMemoryEventStore")

        estore = rewind.construct_eventstore(config, [], "estoresection")

        self.assertIsInstance(estore, eventstores.InMemoryEventStore)


class _RewindRunnerThread(threading.Thread):

    """A thread that runs a rewind instance.

    While the thread is given command line arguments, Rewind is started as
    thread rather than external process. This makes it possible to check code
    coverage and track exit codes etc.

    """

    _EXIT_CODE = b'EXIT'

    def __init__(self, cmdline_args, exit_addr=None):
        """Constructor.

        Parameters:
        cmdline_args -- command line arguments used to execute the rewind.
        exit_addr    -- the ZeroMQ address used to send the exit message to.

        """
        thread = self

        assert '--exit-codeword' not in cmdline_args, \
            "'--exit-codeword' is added by _RewindRunnerThread. Not elsewhere"
        cmdline_args = (['--exit-codeword',
                         _RewindRunnerThread._EXIT_CODE.decode()] +
                        cmdline_args)

        def exitcode_runner(*args, **kwargs):
            try:
                thread.exit_code = rewind.main(*args, **kwargs)
            except SystemExit as e:
                thread.exit_code = e.code
            else:
                # If SystemExit is never thrown Python would have exitted with
                # exit code 0
                thread.exit_code = 0
        super(_RewindRunnerThread, self).__init__(target=exitcode_runner,
                                                  name="test-rewind",
                                                  args=(cmdline_args,))
        self._exit_addr = exit_addr

    def stop(self, context=None):
        """Send a stop message to the event thread."""
        assert self._exit_addr is not None

        if context is None:
            context = zmq.Context(1)
        socket = context.socket(zmq.REQ)
        socket.connect(self._exit_addr)
        socket.send(_RewindRunnerThread._EXIT_CODE)
        time.sleep(0.5)  # Acceptable exit time
        assert not self.isAlive()
        socket.close()


class TestReplication(unittest.TestCase):

    """Test high-level replication behaviour."""

    UUID_REGEXP = ("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                   "[0-9a-f]{12}")

    def setUp(self):
        """Starting a Rewind instance to test replication."""
        args = ['--query-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.rewind = _RewindRunnerThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

        self.context = zmq.Context(3)

        self.transmitter = self.context.socket(zmq.REQ)
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.setsockopt(zmq.SUBSCRIBE, b'')

        self.transmitter.connect('tcp://127.0.0.1:8090')
        self.receiver.connect('tcp://127.0.0.1:8091')

        # Time it takes to connect. This is particularly important so that the
        # receiver does not just receive the tail of the stream.
        time.sleep(0.5)

    def testBasicEventProxying(self):
        """Asserting a single event is proxied."""
        eventid = b"abc12332fffgdgaab134432423"
        eventstring = b"THIS IS AN EVENT"

        self.transmitter.send(b"PUBLISH", zmq.SNDMORE)
        self.transmitter.send(eventstring)
        response = self.transmitter.recv()
        assert not self.transmitter.getsockopt(zmq.RCVMORE)
        assert response == b"PUBLISHED"

        received_id = self.receiver.recv().decode()
        self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
        received_previd = self.receiver.recv().decode()
        self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
        received_string = self.receiver.recv()
        self.assertFalse(self.receiver.getsockopt(zmq.RCVMORE))

        self.assertIsNotNone(re.match(self.UUID_REGEXP, received_id))
        self.assertEqual(received_previd, '')
        self.assertEqual(received_string, eventstring)

    def testProxyingABunchOfEvents(self):
        """Testing that a bunch of incoming messages processed correctly.

        That is, they are all being proxied and in order.

        """
        NMESSAGES = 200
        messages = []
        for id in range(NMESSAGES):
            eventstring = "THIS IS EVENT NUMBER {0}".format(id).encode()
            messages.append(eventstring)

        # Sending
        for msg in messages:
            self.transmitter.send(b"PUBLISH", zmq.SNDMORE)
            self.transmitter.send(msg)
            response = self.transmitter.recv()
            assert not self.transmitter.getsockopt(zmq.RCVMORE)
            assert response == b"PUBLISHED"

        # Receiving and asserting correct messages
        eventids = []
        previd = ''
        for msg in messages:
            received_id = self.receiver.recv().decode()
            self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
            received_previd = self.receiver.recv().decode()
            self.assertTrue(self.receiver.getsockopt(zmq.RCVMORE))
            received_string = self.receiver.recv()
            self.assertFalse(self.receiver.getsockopt(zmq.RCVMORE))

            self.assertIsNotNone(re.match(self.UUID_REGEXP, received_id))
            eventids.append(received_id)
            self.assertEqual(received_string, msg)

            self.assertEqual(received_previd, previd)
            previd = received_id

        self.assertEqual(len(set(eventids)), len(eventids),
                         "Found duplicate event id!")

    def tearDown(self):
        """Shutting down Rewind test instance."""
        self.transmitter.close()
        self.receiver.close()

        self.assertTrue(self.rewind.isAlive(),
                        "Did rewind crash? Not running.")
        self.rewind.stop(self.context)
        self.assertFalse(self.rewind.isAlive(),
                         "Rewind should not have been running. It was.")

        self.context.term()


class TestQuerying(unittest.TestCase):

    """Test high-level event querying behaviour."""

    def setUp(self):
        """Start and populate a Rewind instance to test querying."""

        # Generate artifical events. Doing this at the top of the function so
        # that ZeroMQ gets disappointed that we don't close sockets and stuff
        # if things fails.
        NEVENTS = 200
        events = [uuid.uuid1().hex.encode() for i in range(NEVENTS)]
        self.assertTrue(isinstance(events[0], bytes), type(events[0]))
        self.assertEqual(len(events), len(set(events)),
                         'There were duplicate IDs.'
                         ' Maybe the UUID1 algorithm is flawed?')

        args = ['--query-bind-endpoint', 'tcp://127.0.0.1:8090']
        self.rewind = _RewindRunnerThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

        self.context = zmq.Context(3)

        self.querysock = self.context.socket(zmq.REQ)
        self.querysock.connect('tcp://127.0.0.1:8090')

        self.sent = []
        for event in events:
            # Generating events
            self.querysock.send(b"PUBLISH", zmq.SNDMORE)
            self.querysock.send(event)
            self.sent.append(event)
            response = self.querysock.recv()
            assert response == b"PUBLISHED"
            assert not self.querysock.getsockopt(zmq.RCVMORE)

    def _fetchAllEvents(self):
        """Fetch all events previously put into Rewind.

        Returns a list of events.

        """
        from_ = b''
        to = b''
        result = []
        while True:
            events, capped = self._fetchEvents(from_, to)
            result.extend(events)
            if not capped:
                break
            else:
                from_ = events[-1][0]
        return result

    def _fetchEvents(self, from_=b'', to=b''):
        """Make a ZeroMQ request/query for events.

        Returns a tuple consisting of:
         * a list of event tuple. Could have become capped by the event store
         * if too many events were to be returned. Each tuple consists of:
          * event id (type: bytes)
          * event data (type: bytes)
         * a boolean all events queried for were returned, or whether the
           result was capped. True if all were returned, False if capped.

        """
        self.assertTrue(isinstance(from_, bytes), type(from_))
        self.assertTrue(isinstance(to, bytes), type(to))

        self.querysock.send(b'QUERY', zmq.SNDMORE)  # Command
        self.querysock.send(from_, zmq.SNDMORE)     # From
        self.querysock.send(to)                     # To

        more = True    # Whether more events can be fetched from the response
        capped = True  # Whether events were capped
        events = []
        while more:
            data = self.querysock.recv()
            self.assertFalse(data.startswith(b'ERROR'))

            if data == b'END':
                self.assertFalse(self.querysock.getsockopt(zmq.RCVMORE))
                capped = False
            else:
                eventid = data
                self.assertTrue(isinstance(eventid, bytes), type(eventid))
                self.assertTrue(self.querysock.getsockopt(zmq.RCVMORE))
                eventdata = self.querysock.recv()
                self.assertFalse(eventdata.startswith(b'ERROR'))
                events.append((eventid, eventdata))

            if not self.querysock.getsockopt(zmq.RCVMORE):
                more = False

        return events, capped

    def testSyncAllPastEventsNonCapped(self):
        """Test querying all events."""
        time.sleep(0.5)  # Max time to persist the messages

        events = self._fetchAllEvents()
        events = [ev[1] for ev in events]

        self.assertEqual(self.sent, events)

    def testSyncAllPastEventsCapped(self):
        """Test querying all events."""
        time.sleep(0.5)  # Max time to persist the messages

        events, capped = self._fetchEvents()
        events = [ev[1] for ev in events]

        # Assert we were capped
        self.assertTrue(capped)
        self.assertTrue(len(self.sent) > len(events))

        self.assertEqual(self.sent[:len(events)], events)

    def testSyncEventsSince(self):
        """Test querying events after a certain time."""
        time.sleep(0.5)  # Max time to persist the messages

        allevents = self._fetchAllEvents()
        from_ = allevents[3][0]
        events, capped = self._fetchEvents(from_=from_)
        events = [ev[1] for ev in events]

        # Assert we were capped
        self.assertTrue(capped)
        self.assertTrue(len(self.sent) - 3 > len(events))

        self.assertEqual(self.sent[4:len(events) + 4], events)

    def testSyncEventsBeforeWithCap(self):
        """Test querying events before a certain time. Result is capped."""
        time.sleep(0.5)  # Max time to persist the messages

        allevents = self._fetchAllEvents()
        to = allevents[-3][0]
        events, capped = self._fetchEvents(to=to)
        events = [ev[1] for ev in events]

        # Assert we were capped
        self.assertTrue(capped)
        self.assertEqual(100, len(events),
                         "Expected to have capped events at 100 events.")

        self.assertEqual(self.sent[:100], events)

    def testSyncEventsBeforeNoCap(self):
        """Test querying events before a certain time. Result is not capped."""
        time.sleep(0.5)  # Max time to persist the messages

        allevents = self._fetchAllEvents()
        to = allevents[9][0]
        events, capped = self._fetchEvents(to=to)
        events = [ev[1] for ev in events]

        self.assertFalse(capped)

        self.assertEqual(10, len(events))
        self.assertEqual(self.sent[:10], events)

    def testSyncEventsBetweenWithCap(self):
        """Test querying a slice of the events. Result is capped."""
        time.sleep(0.5)  # Max time to persist the messages

        allevents = self._fetchAllEvents()
        from_ = allevents[3][0]
        to = allevents[-3][0]
        events, capped = self._fetchEvents(from_=from_, to=to)
        events = [ev[1] for ev in events]

        # Assert we were capped
        self.assertTrue(capped)
        self.assertEqual(100, len(events),
                         "Expected to have capped events at 100 events.")
        allevdata = [ev[1] for ev in allevents]

        self.assertEqual(events, [ev[1] for ev in allevents[4:(4 + 100)]])

        self.assertEqual(self.sent[4:len(events) + 4], events)

    def testSyncNontExistentEvent(self):
        """Test when querying for non-existent event id."""
        nonexistentevid = (b'', b'non-exist', b'non-exist2')

        for evid1, evid2 in itertools.permutations(nonexistentevid, 2):
            # Testing  various permutations of non-existing event id queries
            self.querysock.send(b'QUERY', zmq.SNDMORE)  # Command
            self.querysock.send(evid1, zmq.SNDMORE)     # From
            self.querysock.send(evid2)                  # To
            resp = self.querysock.recv()
            self.assertTrue(resp.startswith(b'ERROR '))
            self.assertFalse(self.querysock.getsockopt(zmq.RCVMORE))

    def testUnrecognizedRequest(self):
        """Test how Rewind handles unrecognized request commands."""
        self.querysock.send(b"UNKNOWN COMMAND")
        data = self.querysock.recv()
        self.assertTrue(data.startswith(b'ERROR'))
        self.assertFalse(self.querysock.getsockopt(zmq.RCVMORE))

    def testUnrecognizedEnvelopedRequest(self):
        """Test how Rewind handles unknown enveloped request commands."""
        self.querysock.send(b"UNKNOWN COMMAND", zmq.SNDMORE)
        self.querysock.send(b"additional data")
        data = self.querysock.recv()
        self.assertTrue(data.startswith(b'ERROR'))
        self.assertFalse(self.querysock.getsockopt(zmq.RCVMORE))

    def tearDown(self):
        """Close Rewind test instance."""
        self.querysock.close()

        self.assertTrue(self.rewind.isAlive(),
                        "Did rewind crash? Not running.")
        self.rewind.stop(self.context)
        self.assertFalse(self.rewind.isAlive(),
                         "Rewind should not have been running. It was.")

        self.context.term()


class TestIdGenerator(unittest.TestCase):

    """Test `_IdGenerator`."""

    def testRegenerationIfKeyExists(self):
        """Test key-exist-check on regeneration.

        Regeneration is done so seldom in regular tests that this check needs
        to be executed specifically.

        """
        key_checker = mock.Mock()
        key_checker.side_effect = [True, True, True, False]

        generator = rewind._IdGenerator(key_checker)
        key = generator.generate()

        # Assert the returns key was checked for existence
        key_checker.assert_called_with(key)

        self.assertEqual(key_checker.call_count, 4)
