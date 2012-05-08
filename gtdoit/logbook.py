import sys
import argparse
import sys
import logging
import uuid
import itertools
import contextlib

import zmq

import gtdoit.messages.events_pb2 as events_pb2
import gtdoit.messages.eventhandling_pb2 as eventhandling_pb2


logger = logging.getLogger(__name__)


class LogBookKeyError(KeyError):
    pass


class LogBookEventOrderError(IndexError):
    pass


class EventStore(object):
    """Stores events and keeps track of their order.
    
    Thread safe.

    TODO: Currently this uses an in-memory implementation. Needs to be
    persisted.
    """
    class EventKeyAlreadyExistError(LogBookKeyError):
        pass

    class EventKeyDoesNotExistError(LogBookKeyError):
        pass

    class EventIndexError(IndexError):
        pass

    def __init__(self):
        self._reset()

    def _reset(self):
        """Reset the event storage.

        Protected/private because it should only be called from tests.

        Calling this method makes it thread unsafe.
        """
        self.keys = []
        self.events = {}

    def add_event(self, key, event):
        if key in self.keys or key in self.events:
            raise EventKeyAlreadyExistError("The key already existed: %s" % key)
        self.keys.append(key)
        # Important no exceptions happen between these lines!
        self.events[key] = event

    def get_events(self, from_=None, to=None):
        if from_ and (from_ not in self.keys or from_ not in self.events):
            raise EventKeyDoesNotExistError("Could not find the from_ key: %s" %
                                            from_)
        if to and (to not in self.keys or to not in self.events):
            raise EventKeyDoesNotExistError("Could not find the from_ key: %s" %
                                            to)
        
        # +1 here because we have already seen the event we are asking for
        fromindex = self.keys.index(from_)+1 if from_ else 0

        toindex = self.keys.index(to)+1 if to else len(self.events)
        if fromindex > toindex:
            raise LogBookEventOrderError("'From' index came after 'To'. \
                                         Keys: (%s, %s) \
                                         Indices: (%s, %s)" % (from_, to,
                                                               fromindex,
                                                               toindex))
        return (self.events[key] for key in self.keys[fromindex:toindex])

    def key_exists(self, key):
        return key in self.keys


class IdGenerator:
    """Generates unique strings."""
    def __init__(self, key_exists=lambda key: False):
        self.key_exists = key_exists

    def _propose_new_key(self):
        return str(uuid.uuid4())

    def generate(self):
        key = self._propose_new_key()
        while self.key_exists(key):
            logger.warning('Previous candidate was used.'
                           ' Regenerating another...')
            key = self._propose_new_key()
        return key


class LogBookRunner(object):
    """ Helper class for splitting the runnable part of Logbook into logical
        parts.

    The state of the class is the state necessary to execute the application.
    """
    def __init__(self, eventstore, incoming_socket, query_socket,
                 streaming_socket, exit_message=None):
        """Constructor."""
        self.eventstore = eventstore
        self.incoming_socket = incoming_socket
        self.query_socket = query_socket
        self.streaming_socket = streaming_socket
        self.exit_message = exit_message

        self.id_generator = IdGenerator(key_exists=lambda key:
                                        eventstore.key_exists(key))

        # Initialize poll set
        self.poller = zmq.Poller()
        self.poller.register(incoming_socket, zmq.POLLIN)
        self.poller.register(query_socket, zmq.POLLIN)

    def run(self):
        """ Actually run the program infinitely, or until an exit message is
            received.
        """
        while self._handle_one_message():
            pass

    def _handle_one_message(self):
        """Handle one single incoming message on any socket.

        This is the inner loop of the main application loop.
        """
        socks = dict(self.poller.poll())

        if self.incoming_socket in socks \
           and socks[self.incoming_socket]==zmq.POLLIN:
            return self._handle_incoming_event()
        elif self.query_socket in socks \
                and socks[self.query_socket]==zmq.POLLIN:
            return self._handle_query()

    def _handle_incoming_event(self):
        """Handle an incoming event."""
        eventstr = self.incoming_socket.recv()

        if self.exit_message and eventstr==self.exit_message:
            return False

        event = events_pb2.Event()
        event.ParseFromString(eventstr)
        newid = self.id_generator.generate()
        stored_event = eventhandling_pb2.StoredEvent(eventid=newid,
                                                     event=event)

        # Only serializing once
        stored_event_str = stored_event.SerializeToString()

        self.eventstore.add_event(newid, stored_event_str)
        self.streaming_socket.send(eventstr)

        return True

    def _handle_query(self):
        """Handle an event query."""
        reqstr = self.query_socket.recv()
        request = eventhandling_pb2.EventRequest()
        request.ParseFromString(reqstr)
        request_types = eventhandling_pb2.EventRequest
        if request.type == request_types.RANGE_STREAM_REQUEST:
            fro = request.event_range.fro
            to = request.event_range.to
            events = self.eventstore.get_events(from_=fro, to=to)

            # Since we are using ZeroMQ enveloping we want to cap the
            # maximum number of messages that are send for each request.
            # Otherwise we might run out of memory for a lot of memory.
            MAX_ELMNTS_PER_REQ = 100
            events = itertools.islice(events, 0, MAX_ELMNTS_PER_REQ+1)
            events = list(events)
            if len(events)==MAX_ELMNTS_PER_REQ+1:
                # There are more elements, but we are capping the result
                for event in events[:-1]:
                    self.query_socket.send(event, zmq.SNDMORE)
                self.query_socket.send(events[-1])
            else:
                # Sending all events. Ie., we are not capping
                for event in events:
                    self.query_socket.send(event, zmq.SNDMORE)
                self.query_socket.send("END")

        return True


@contextlib.contextmanager
def zmq_context_context(*args):
    """A ZeroMQ context context that both constructs and terminates it."""
    context = zmq.Context(*args)
    try:
        yield context
    finally:
        context.term()


@contextlib.contextmanager
def zmq_socket_context(context, socket_type, bind_endpoints, connect_endpoints):
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
    """Actually execute the program."""
    eventstore = EventStore()

    with zmq_context_context(3) as context, \
            zmq_socket_context(context, zmq.PULL, args.incoming_bind_endpoints,
                               args.incoming_connect_endpoints) \
                as incoming_socket, \
            zmq_socket_context(context, zmq.REP, args.query_bind_endpoints,
                               args.query_connect_endpoints) \
                as query_socket, \
            zmq_socket_context(context, zmq.PUB, args.streaming_bind_endpoints,
                               args.streaming_connect_endpoints) \
                as streaming_socket:
        # Executing the program in the context of ZeroMQ context as well as
        # ZeroMQ sockets. Using with here to make sure are correctly closing
        # things in the correct order, particularly also if we have an exception
        # or similar.

        runner = LogBookRunner(eventstore, incoming_socket, query_socket,
                               streaming_socket, args.exit_message)
        runner.run()

    return 0


def main(argv=None, exit=True):
    """Entry point for the logbook.

    Parses input and calls run() for the real work.

    Parameters:
        argv -- sys.argv arguments. Can be set for testing purposes.
        exit -- whether to call sys.exit(...) when this function is done, or
                not.

    returns -- the return code of the programif exit is set to True. Otherwise
               it exits the Python interpreter before returning.
    """
    parser = argparse.ArgumentParser(
        description='Event storage and event proxy.'
    )
    parser.add_argument('--exit-codeword', metavar="MSG", dest="exit_message",
                        help="An incoming message that makes the logbook quit."
                             " Used for testing.")

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
                              help='the connect address for streaming of events',
                              action='append',
                              dest='streaming_connect_endpoints')

    args = argv if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(args)
    except SystemExit as e:
        if exit:
            raise e
        else:
            return e.code

    if not args.incoming_bind_endpoints \
       and not args.incoming_connect_endpoints \
       and not args.query_bind_endpoints \
       and not args.query_connect_endpoints:
        errmsg = "You must either specify an incoming or query endpoint.\n" \
                "(there's no use in simply having a streaming endpoint)"
        if exit:
            parser.error(errmsg)
        else:
            print errmsg
            return 2

    exitcode = run(args)

    if exit:
        sys.exit(exitcode)
    else:
        return exitcode
