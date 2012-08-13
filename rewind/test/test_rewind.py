from __future__ import print_function
import contextlib
import itertools
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

import zmq

import rewind.communicators as communicators
import rewind.rewind as rewind


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
        # See tearDown() why this one is defined
        self.rewind = None

    def tearDown(self):
        if self.rewind and self.rewind.isAlive():
            # Making sure to close rewind if it has been defined
            self.rewind.stop()
            self.rewind = None

    def testAtLeastOneEndpointRequired(self):
        with _direct_stderr_to_stdout():
            rewind = _LogbookThread([])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 2)

    def testOnlyStreamingEndpointFails(self):
        with _direct_stderr_to_stdout():
            rewind = _LogbookThread(['--streaming-bind-endpoint',
                                     'tcp://hello'])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 2)

    def testHelp(self):
        with _direct_stderr_to_stdout():
            rewind = _LogbookThread(['--help'])
            rewind.start()
            rewind.join(2)
        self.assertFalse(rewind.isAlive())
        self.assertEqual(rewind.exit_code, 0)

    def testStartingWithPersistence(self):
        datapath = tempfile.mkdtemp()
        print("Using datapath:", datapath)

        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--streaming-bind-endpoint', 'tcp://127.0.0.1:8091',
                '--datadir', datapath]
        print(" ".join(args))
        self.rewind = _LogbookThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

        time.sleep(3)
        self.assertTrue(self.rewind.isAlive(),
                        "Rewind was not running for more than 3 seconds")

        # Not removing this in tearDown for two reasons:
        # 1. Datapath is not created in setUp()
        # 2. If this test fails, we will keep the datapath that was created.
        shutil.rmtree(datapath)


class _LogbookThread(threading.Thread):
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
               "'--exit-codeword' is added by _LogbookThread. Not elsewhere"
        cmdline_args = (['--exit-codeword',
                         _LogbookThread._EXIT_CODE.decode()] +
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
        super(_LogbookThread, self).__init__(target=exitcode_runner,
                                             name="test-rewind",
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
        self.rewind = _LogbookThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

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

        received_id = self.receiver.recv().decode()
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
            received_id = self.receiver.recv().decode()
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

        self.assertTrue(self.rewind.isAlive(),
                        "Did rewind crash? Not running.")
        self.rewind.stop(self.context)
        self.assertFalse(self.rewind.isAlive(),
                         "Logbook should not have been running. It was.")

        self.context.term()


class TestLogbookQuerying(unittest.TestCase):
    def setUp(self):
        args = ['--incoming-bind-endpoint', 'tcp://127.0.0.1:8090',
                '--query-bind-endpoint', 'tcp://127.0.0.1:8091']
        self.rewind = _LogbookThread(args, 'tcp://127.0.0.1:8090')
        self.rewind.start()

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

        self.assertTrue(self.rewind.isAlive(),
                        "Did rewind crash? Not running.")
        self.rewind.stop(self.context)
        self.assertFalse(self.rewind.isAlive(),
                         "Logbook should not have been running. It was.")

        self.context.term()