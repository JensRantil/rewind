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
import itertools
import logging
import os
import sys
import types
import uuid

import zmq

import rewind.server.config as config
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
            try:
                socket.bind(endpoint)
            except Exception:
                _logger.fatal("Could not bind to '%s'.", endpoint)
                raise
        yield socket
    finally:
        socket.close()


def _get_with_fallback(config, section, option, fallback):
    """Get a configuration value, using fallback for missing values.

    Parameters:
    config   -- the configparser to try to extract the option value from.
    section  -- the section to extract value from.
    option   -- the name of the option to extract value from.
    fallback -- fallback value to return if no value was set in `config`.

    returns  -- the config option value if it was set, else fallback.

    """
    exists = (config.has_section(section)
              and config.has_option(section, option))

    if not exists:
        return fallback
    else:
        return config.get(section, option)


def run(options, exit_codeword=None):
    """Actually execute the program.

    Calling this method can be done from tests to simulate executing the
    application from command line.

    Parameters:
    options       -- `optionparser` from config file.
    exit_codeword -- an optional exit_message that will shut down Rewind. Used
                     for testing.

    returns -- exit code for the application. Non-zero for errors.

    """
    QUERY_ENDP_OPT = 'query-bind-endpoint'
    STREAM_ENDP_OPT = 'streaming-bind-endpoint'
    ZMQ_NTHREADS = "zmq-nthreads"

    if not options.has_section(config.DEFAULT_SECTION):
        msg = "Missing default section, `{0}`."
        fmsg = msg.format(config.DEFAULT_SECTION)
        raise config.ConfigurationError(fmsg)

    if not options.has_option(config.DEFAULT_SECTION, QUERY_ENDP_OPT):
        msg = "Missing (query) bind endpoint in option file: {0}:{1}"
        fmsg = msg.format(config.DEFAULT_SECTION, QUERY_ENDP_OPT)
        raise config.ConfigurationError(fmsg)

    queryendp = options.get(config.DEFAULT_SECTION, QUERY_ENDP_OPT).split(",")
    streamendp = _get_with_fallback(options, config.DEFAULT_SECTION,
                                    STREAM_ENDP_OPT, '').split(",")
    queryendp = filter(lambda x: x.strip(), queryendp)
    streamendp = filter(lambda x: x.strip(), streamendp)

    try:
        eventstore = config.construct_eventstore(options)
    except config.ConfigurationError as e:
        _logger.exception("Could instantiate event store from config file.")
        raise

    zmq_nthreads = _get_with_fallback(options, config.DEFAULT_SECTION,
                                      ZMQ_NTHREADS, '3')
    try:
        zmq_nthreads = int(zmq_nthreads)
    except ValueError:
        msg = "{0}:{1} must be an integer".format(config.DEFAULT_SECTION,
                                                  ZMQ_NTHREADS)
        _logger.fatal(msg)
        return 1

    with _zmq_context_context(zmq_nthreads) as context, \
            _zmq_socket_context(context, zmq.REP, queryendp) as querysock, \
            _zmq_socket_context(context, zmq.PUB,
                                streamendp) as streamsock:
        # Executing the program in the context of ZeroMQ context as well as
        # ZeroMQ sockets. Using with here to make sure are correctly closing
        # things in the correct order, particularly also if we have an
        # exception or similar.

        runner = _RewindRunner(eventstore, querysock, streamsock,
                               (exit_codeword.encode()
                                if exit_codeword
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
        description='Event storage and event proxy.',
        usage='%(prog)s <configfile>'
    )
    parser.add_argument('--exit-codeword', metavar="MSG", dest="exit_message",
                        default=None, help="An incoming message that makes"
                                           " Rewind quit. Used for testing.")
    parser.add_argument('configfile')

    args = argv if argv is not None else sys.argv[1:]
    args = parser.parse_args(args)

    config = configparser.SafeConfigParser()
    with open(args.configfile) as f:
        config.readfp(f)

    exitcode = run(config, args.exit_message)
    return exitcode
