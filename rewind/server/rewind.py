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
try:
    # Python < 3
    import ConfigParser as configparser
except ImportError:
    # Python >= 3
    import configparser
import contextlib
import hashlib
import importlib
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

    def __init__(self, eventstore, query_socket, streaming_socket,
                 exit_message=None):
        """Constructor."""
        self.eventstore = eventstore
        self.query_socket = query_socket
        self.streaming_socket = streaming_socket
        assert exit_message is None or isinstance(exit_message, bytes)
        self.exit_message = exit_message
        self.oldid = ''

        self.id_generator = _IdGenerator(key_exists=lambda key:
                                         eventstore.key_exists(key))

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

        It is crucial that this class function always respond with a
        query_socket.sent() for every query_socket.recv() call. Otherwise,
        clients and/or server might be stuck in limbo.

        """
        result = True

        requesttype = self.query_socket.recv()

        if requesttype == b"PUBLISH":
            self._handle_incoming_event()
        elif requesttype == b"QUERY":
            self._handle_event_query()
        elif (self.exit_message is not None
              and requesttype == self.exit_message):
            _logger.warn("Asked to quit through an exit message."
                         "I'm quitting...")
            self.query_socket.send(b'QUIT')
            result = False
        else:
            _logger.warn("Could not identify request type: %s", requesttype)
            self._handle_unknown_command()

        return result

    def _handle_unknown_command(self):
        """Handle an unknown RES command.

        The function makes sure to recv all message parts and respond with an
        error.

        """
        while self.query_socket.getsockopt(zmq.RCVMORE):
            # Making sure we 'empty' enveloped message. Otherwise, we can't
            # respond.
            self.query_socket.recv()
        self.query_socket.send(b"ERROR Unknown request type")

    def _handle_event_query(self):
        """Handle an incoming event query."""
        assert self.query_socket.getsockopt(zmq.RCVMORE)
        fro = self.query_socket.recv().decode()
        assert self.query_socket.getsockopt(zmq.RCVMORE)
        to = self.query_socket.recv().decode()
        assert not self.query_socket.getsockopt(zmq.RCVMORE)

        _logger.debug("Incoming query: (from, to)=(%s, %s)", fro, to)

        try:
            events = self.eventstore.get_events(fro if fro else None,
                                                to if to else None)
        except eventstores.EventStore.EventKeyDoesNotExistError:
            _logger.exception("A client requested a key that does not"
                              " exist:")
            self.query_socket.send(b"ERROR Key did not exist")
            return

        # Since we are using ZeroMQ enveloping we want to cap the
        # maximum number of messages that are send for each request.
        # Otherwise we might run out of memory for a lot of events.
        MAX_ELMNTS_PER_REQ = 100

        events = itertools.islice(events, 0, MAX_ELMNTS_PER_REQ)
        events = list(events)
        if len(events) == MAX_ELMNTS_PER_REQ:
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

    def _handle_incoming_event(self):
        """Handle an incoming event.

        Returns True if further messages should be received, False otherwise
        (it should quit, that is).

        TODO: Move the content of this function into `_handle_one_message`.
        This class function does not simply handle incoming events.

        """
        eventstr = self.query_socket.recv()

        newid = self.id_generator.generate()

        # Make sure newid is not part of our request vocabulary
        assert newid not in (b"QUERY", b"PUBLISH"), \
            "Generated ID must not be part of req/rep vocabulary."
        assert not newid.startswith("ERROR"), \
            "Generated ID must not be part of req/rep vocabulary."

        # Important this is done before forwarding to the streaming socket
        self.eventstore.add_event(newid, eventstr)

        self.streaming_socket.send(newid.encode(), zmq.SNDMORE)
        self.streaming_socket.send(self.oldid.encode(), zmq.SNDMORE)
        self.streaming_socket.send(eventstr)

        self.oldid = newid

        assert not self.query_socket.getsockopt(zmq.RCVMORE)
        self.query_socket.send(b"PUBLISHED")


@contextlib.contextmanager
def _zmq_context_context(*args):
    """A ZeroMQ context context that both constructs and terminates it."""
    context = zmq.Context(*args)
    try:
        yield context
    finally:
        context.term()


@contextlib.contextmanager
def _zmq_socket_context(context, socket_type, bind_endpoints):
    """A ZeroMQ socket context that both constructs a socket and closes it."""
    socket = context.socket(socket_type)
    try:
        for endpoint in bind_endpoints:
            socket.bind(endpoint)
        yield socket
    finally:
        socket.close()


class _ConfigurationError(Exception):

    """An error thrown when configuration of the application fails."""

    def __init__(self, what):
        self.what = what

    def __str__(self):
        return repr(self.what)


def construct_eventstore(config, args, section=None):
    """Construct the event store to write and write from/to.

    The event store is constructed from an optionally given configuration file
    and/or command line arguments. Command line arguments has higher
    presendence over configuration file attributes.

    If a defined event store is missing, `InMemoryEventStore` will be used and
    a warning will be logged to stderr.

    This function is considered public API since external event stores might
    use it to load other (usually, underlying) event stores.

    Arguments:
    config  -- a configuration dictionary. Derived from
                     `configparser.RawConfigParser`.
    args    -- the arguments given at command line to Rewind.
    section -- the config section to use for event store instantiation.

    returns -- a new event store.

    """
    DEFAULT_SECTION = 'general'
    ESTORE_CLASS_ATTRIBUTE = 'class'

    if config is None:
        _logger.warn("Using InMemoryEventStore. Events are not persisted."
                     " See example config file on how to persist them.")
        eventstore = eventstores.InMemoryEventStore()
        return eventstore

    if section is None:
        if DEFAULT_SECTION not in config.sections():
            raise _ConfigurationError("Missing default section, `general`.")
        section = config.get(DEFAULT_SECTION, 'storage-backend')

    if section not in config.sections():
        msg = "The section for event store does not exist: {0}"
        raise _ConfigurationError(msg.format(section))

    if not config.has_option(section, ESTORE_CLASS_ATTRIBUTE):
        errmsg = 'Configuration option `class` missing for section `{0}`.'
        raise _ConfigurationError(errmsg.format(section))

    classpath = config.get(section, ESTORE_CLASS_ATTRIBUTE)
    classpath_pieces = classpath.split('.')
    classname = classpath_pieces[-1]
    modulepath = '.'.join(classpath_pieces[0:-1])

    module = importlib.import_module(modulepath)

    # Instantiating the event store in question using custom arguments
    options = config.options(section)
    if ESTORE_CLASS_ATTRIBUTE in options:
        # Unnecessary argument
        i = options.index(ESTORE_CLASS_ATTRIBUTE)
        del options[i]
    Class = getattr(module, classname)
    customargs = {option: config.get(section, option) for option in options}
    eventstore = Class.from_config(config, args, **customargs)

    return eventstore


def run(args):
    """Actually execute the program.

    Calling this method can be done from tests to simulate executing the
    application from command line.

    Parameters:
    args    -- a list of command line parameters (omitting the initial program
               list item given in `sys.argv`.

    returns -- a proposed exit code for the application.

    """
    if args.configfile is not None:
        config = configparser.SafeConfigParser()
        # TODO: Add a default config file to try to read from home, and etc,
        # directory?
        config.read([args.configfile])
    else:
        config = None

    try:
        eventstore = construct_eventstore(config, args)
    except _ConfigurationError as e:
        _logger.exception("Could instantiate event store from config file.")
        return

    with _zmq_context_context(3) as context, \
            _zmq_socket_context(context, zmq.REP, args.query_bind_endpoints) \
            as query_socket, \
            _zmq_socket_context(context, zmq.PUB,
                                args.streaming_bind_endpoints) \
            as streaming_socket:
        # Executing the program in the context of ZeroMQ context as well as
        # ZeroMQ sockets. Using with here to make sure are correctly closing
        # things in the correct order, particularly also if we have an
        # exception or similar.

        runner = _RewindRunner(eventstore, query_socket,
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
    parser.add_argument('--configfile', '-c', metavar='FILE',
                        help="Configuration file.")

    query_group = parser.add_argument_group(
        title='Querying endpoints',
        description='Endpoints listening for event queries.'
    )
    query_group.add_argument('--query-bind-endpoint',
                             metavar='ZEROMQ-ENDPOINT', default=[],
                             help='the bind address for querying of events',
                             action='append', dest='query_bind_endpoints')
    stream_group = parser.add_argument_group(
        title='Streaming endpoints',
        description='Endpoints for streaming incoming events.'
    )
    stream_group.add_argument('--streaming-bind-endpoint',
                              metavar='ZEROMQ-ENDPOINT', default=[],
                              help='the bind address for streaming of events',
                              action='append',
                              dest='streaming_bind_endpoints')

    args = argv if argv is not None else sys.argv[1:]
    args = parser.parse_args(args)

    if not args.query_bind_endpoints:
        errmsg = ("You must specify a query endpoint.\n"
                  "(there's no use in simply having a streaming endpoint)")
        parser.error(errmsg)

    exitcode = run(args)
    return exitcode
