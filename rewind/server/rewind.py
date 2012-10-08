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

"""Main executable of Rewind."""
from __future__ import print_function
from __future__ import absolute_import
import argparse
import contextlib
import hashlib
import itertools
import logging
import os
import sys
import types
import uuid

import zmq

import rewind.server.eventstores as eventstores


_logger = logging.getLogger(__name__)


class _IdGenerator:

    """Generates unique strings."""

    def __init__(self, key_exists=lambda key: False):
        self.key_exists = key_exists

    def _propose_new_key(self):
        return str(uuid.uuid4())

    def generate(self):
        """Generate a new string and return it."""
        key = self._propose_new_key()
        while self.key_exists(key):
            _logger.warning('Previous candidate was used.'
                            ' Regenerating another...')
            key = self._propose_new_key()
        return key


class _RewindRunner(object):

    """Message juggler.

    Receives events, stores and proxies them. Also handles queries and responds
    to them.

    The state of the class is the state necessary to execute the application.

    """

    def __init__(self, eventstore, incoming_socket, query_socket,
                 streaming_socket, exit_message=None):
        """Constructor."""
        self.eventstore = eventstore
        self.incoming_socket = incoming_socket
        self.query_socket = query_socket
        self.streaming_socket = streaming_socket
        assert exit_message is None or isinstance(exit_message, bytes)
        self.exit_message = exit_message
        self.oldid = ''

        self.id_generator = _IdGenerator(key_exists=lambda key:
                                         eventstore.key_exists(key))

        # Initialize poll set
        self.poller = zmq.Poller()
        self.poller.register(incoming_socket, zmq.POLLIN)
        self.poller.register(query_socket, zmq.POLLIN)

    def run(self):
        """Main loop for `_RewindRunner`.

        Runs the program infinitely, or until an exit message is received.

        """
        while self._handle_one_message():
            pass

    def _handle_one_message(self):
        """Handle one single incoming message on any socket.

        This is the inner loop of the main application loop.

        Returns True if further messages should be received, False otherwise
        (it should quit, that is).

        """
        socks = dict(self.poller.poll())

        if (self.incoming_socket in socks and
                socks[self.incoming_socket] == zmq.POLLIN):
            return self._handle_incoming_event()
        elif (self.query_socket in socks
              and socks[self.query_socket] == zmq.POLLIN):
            return self._handle_query()

    def _handle_incoming_event(self):
        """Handle an incoming event.

        Returns True if further messages should be received, False otherwise
        (it should quit, that is).

        """
        eventstr = self.incoming_socket.recv()

        if self.exit_message and eventstr == self.exit_message:
            return False

        newid = self.id_generator.generate()

        # Make sure newid is not part of our request vocabulary
        assert newid != "QUERY", \
            "Generated ID must not be part of req/rep vocabulary."
        assert not newid.startswith("ERROR"), \
            "Generated ID must not be part of req/rep vocabulary."

        # Important this is done before forwarding to the streaming socket
        self.eventstore.add_event(newid, eventstr)

        self.streaming_socket.send(newid.encode(), zmq.SNDMORE)
        self.streaming_socket.send(self.oldid.encode(), zmq.SNDMORE)
        self.streaming_socket.send(eventstr)

        self.oldid = newid

        return True

    def _handle_query(self):
        """Handle an event query.

        Returns True if further messages should be received, False otherwise
        (it should quit, that is).

        """
        requesttype = self.query_socket.recv()
        if requesttype == b"QUERY":
            assert self.query_socket.getsockopt(zmq.RCVMORE)
            fro = self.query_socket.recv().decode()
            assert self.query_socket.getsockopt(zmq.RCVMORE)
            to = self.query_socket.recv().decode()
            assert not self.query_socket.getsockopt(zmq.RCVMORE)

            _logger.debug("Incoming query: (from, to)=(%s, %s)", fro, to)

            try:
                events = self.eventstore.get_events(fro if fro else None,
                                                    to if to else None)
            except eventstores.EventStore.EventKeyDoesNotExistError as e:
                _logger.exception("A client requested a key that does not"
                                  " exist:")
                self.query_socket.send(b"ERROR Key did not exist")
                return True

            # Since we are using ZeroMQ enveloping we want to cap the
            # maximum number of messages that are send for each request.
            # Otherwise we might run out of memory for a lot of events.
            MAX_ELMNTS_PER_REQ = 100

            events = itertools.islice(events, 0, MAX_ELMNTS_PER_REQ + 1)
            events = list(events)
            if len(events) == MAX_ELMNTS_PER_REQ + 1:
                # There are more elements, but we are capping the result
                for eventid, eventdata in events[:-1]:
                    self.query_socket.send(eventid.encode(), zmq.SNDMORE)
                    self.query_socket.send(eventdata, zmq.SNDMORE)
                lasteventid, lasteventdata = events[-1]
                self.query_socket.send(lasteventid.encode(), zmq.SNDMORE)
                self.query_socket.send(lasteventdata)
            else:
                # Sending all events. Ie., we are not capping
                for eventid, eventdata in events:
                    self.query_socket.send(eventid.encode(), zmq.SNDMORE)
                    self.query_socket.send(eventdata, zmq.SNDMORE)
                self.query_socket.send(b"END")
        else:
            logging.warn("Could not identify request type: %s", requesttype)
            self.query_socket.send("ERROR Unknown request type")

        return True


@contextlib.contextmanager
def _zmq_context_context(*args):
    """A ZeroMQ context context that both constructs and terminates it."""
    context = zmq.Context(*args)
    try:
        yield context
    finally:
        context.term()


@contextlib.contextmanager
def _zmq_socket_context(context, socket_type, bind_endpoints,
                        connect_endpoints):
    """A ZeroMQ socket context that both constructs a socket and closes it."""
    socket = context.socket(socket_type)
    try:
        for endpoint in bind_endpoints:
            socket.bind(endpoint)
        for endpoint in connect_endpoints:
            socket.connect(endpoint)
        yield socket
    finally:
        socket.close()


def run(args):
    """Actually execute the program.

    Calling this method can be done from tests to simulate executing the
    application from command line.

    Parameters:
    args    -- a list of command line parameters (omitting the initial program
               list item given in `sys.argv`.

    returns -- a proposed exit code for the application.

    """
    if args.datadir:
        dbdir = os.path.join(args.datadir, 'db')
        def db_creator(filename):
            return eventstores.SQLiteEventStore(filename)
        rotated_db_estore = eventstores.RotatedEventStore(db_creator, dbdir,
                                                          'sqlite')

        logdir = os.path.join(args.datadir, 'appendlog')
        def log_creator(filename):
            return eventstores.LogEventStore(filename)
        rotated_log_estore = eventstores.RotatedEventStore(log_creator,
                                                           logdir,
                                                           'appendlog')

        EVENTS_PER_BATCH = 30000
        eventstore = eventstores.SyncedRotationEventStores(EVENTS_PER_BATCH)

        # Important DB event store is added first. Otherwise, fast event
        # querying will not be enabled.
        eventstore.add_rotated_store(rotated_db_estore)
        eventstore.add_rotated_store(rotated_log_estore)

        # TODO: Make sure event stores are correctly mirrored
    else:
        _logger.warn("Using InMemoryEventStore. Events are not persisted."
                     " See --datadir parameter for further info.")
        eventstore = eventstores.InMemoryEventStore()

    with _zmq_context_context(3) as context, \
            _zmq_socket_context(context, zmq.PULL,
                                args.incoming_bind_endpoints,
                                args.incoming_connect_endpoints) \
            as incoming_socket, \
            _zmq_socket_context(context, zmq.REP, args.query_bind_endpoints,
                                args.query_connect_endpoints) \
            as query_socket, \
            _zmq_socket_context(context, zmq.PUB,
                                args.streaming_bind_endpoints,
                                args.streaming_connect_endpoints) \
            as streaming_socket:
        # Executing the program in the context of ZeroMQ context as well as
        # ZeroMQ sockets. Using with here to make sure are correctly closing
        # things in the correct order, particularly also if we have an
        # exception or similar.

        runner = _RewindRunner(eventstore, incoming_socket, query_socket,
                               streaming_socket, (args.exit_message.encode()
                                                  if args.exit_message
                                                  else None))
        runner.run()

    return 0


def main(argv=None):
    """Entry point for Rewind.

    Parses input and calls run() for the real work.

    Parameters:
    argv    -- sys.argv arguments. Can be set for testing purposes.

    returns -- the proposed exit code for the program.

    """
    parser = argparse.ArgumentParser(
        description='Event storage and event proxy.'
    )
    parser.add_argument('--exit-codeword', metavar="MSG", dest="exit_message",
                        help="An incoming message that makes Rewind quit."
                             " Used for testing.")
    parser.add_argument('--datadir', '-D', metavar="DIR",
                        help="The directory where events will be persisted."
                             " Will be created if non-existent. Without this"
                             " parameter, events will be stored in-memory"
                             " only.")

    incoming_group = parser.add_argument_group(
        title='Incoming event endpoints',
        description='ZeroMQ endpoint for incoming events.'
    )
    incoming_group.add_argument('--incoming-bind-endpoint', action='append',
                                metavar='ZEROMQ-ENDPOINT', default=[],
                                help='the bind address for incoming events',
                                dest='incoming_bind_endpoints')
    incoming_group.add_argument('--incoming-connect-endpoint', action='append',
                                metavar='ZEROMQ-ENDPOINT', default=[],
                                help='the connect address for incoming events',
                                dest='incoming_connect_endpoints')
    query_group = parser.add_argument_group(
        title='Querying endpoints',
        description='Endpoints listening for event queries.'
    )
    query_group.add_argument('--query-bind-endpoint',
                             metavar='ZEROMQ-ENDPOINT', default=[],
                             help='the bind address for querying of events',
                             action='append', dest='query_bind_endpoints')
    query_group.add_argument('--query-connect-endpoint',
                             metavar='ZEROMQ-ENDPOINT', default=[],
                             help='the connect address for querying of events',
                             action='append', dest='query_connect_endpoints')
    stream_group = parser.add_argument_group(
        title='Streaming endpoints',
        description='Endpoints for streaming incoming events.'
    )
    stream_group.add_argument('--streaming-bind-endpoint',
                              metavar='ZEROMQ-ENDPOINT', default=[],
                              help='the bind address for streaming of events',
                              action='append',
                              dest='streaming_bind_endpoints')
    stream_group.add_argument('--streaming-connect-endpoint',
                              metavar='ZEROMQ-ENDPOINT', default=[],
                              help=('the connect address for streaming of '
                                    'events'),
                              action='append',
                              dest='streaming_connect_endpoints')

    args = argv if argv is not None else sys.argv[1:]
    args = parser.parse_args(args)

    if (not args.incoming_bind_endpoints and
            not args.incoming_connect_endpoints and
            not args.query_bind_endpoints and
            not args.query_connect_endpoints):
        errmsg = ("You must either specify an incoming or query endpoint.\n"
                  "(there's no use in simply having a streaming endpoint)")
        if exit:
            parser.error(errmsg)
        else:
            print(errmsg)
            return 2

    exitcode = run(args)
    return exitcode
