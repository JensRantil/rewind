import sys
import argparse
import sys
import logging
import uuid
import itertools

import zmq

import gtdoit.messages.events_pb2 as events_pb2
import gtdoit.messages.eventhandling_pb2 as eventhandling_pb2


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
            # TODO: Make sure that we don't end up in a infinite loop here
            key = self._propose_new_key()
        return key


def build_socket(context, socket_type, bind_endpoints=[], connect_endpoints=[]):
    socket = context.socket(socket_type)
    for endpoint in bind_endpoints:
        socket.bind(endpoint)
    for endpoint in connect_endpoints:
        socket.connect(endpoint)
    return socket


def run(args):
    """Actually execute the program."""
    context = zmq.Context(1)
    incoming_socket = build_socket(context, zmq.PULL,
                                   bind_endpoints=args.incoming_bind_endpoints,
                                   connect_endpoints=args.incoming_connect_endpoints)
    query_socket = build_socket(context, zmq.REP,
                                bind_endpoints=args.query_bind_endpoints,
                                connect_endpoints=args.query_connect_endpoints)
    streaming_socket = build_socket(context, zmq.PUB,
                                    bind_endpoints=args.streaming_bind_endpoints,
                                    connect_endpoints=args.streaming_connect_endpoints)

    eventstore = EventStore()
    id_generator = IdGenerator(key_exists=lambda key:
                               eventstore.key_exists(key))

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(incoming_socket, zmq.POLLIN)
    poller.register(query_socket, zmq.POLLIN)

    while True:
        socks = dict(poller.poll())

        if incoming_socket in socks and socks[incoming_socket]==zmq.POLLIN:
            eventstr = incoming_socket.recv()

            if args.exit_message and eventstr==args.exit_message:
                break

            event = events_pb2.Event()
            event.ParseFromString(eventstr)
            newid = id_generator.generate()
            stored_event = eventhandling_pb2.StoredEvent(eventid=newid,
                                                         event=event)

            # Only serializing once
            stored_event_str = stored_event.SerializeToString()

            eventstore.add_event(newid, stored_event_str)
            streaming_socket.send(eventstr)

        if query_socket in socks and socks[query_socket]==zmq.POLLIN:
            reqstr = query_socket.recv()
            request = eventhandling_pb2.EventRequest()
            request.ParseFromString(reqstr)
            request_types = eventhandling_pb2.EventRequest
            if request.type == request_types.RANGE_STREAM_REQUEST:
                fro = request.event_range.fro
                to = request.event_range.to
                events = eventstore.get_events(from_=fro, to=to)

                # Since we are using ZeroMQ enveloping we want to cap the
                # maximum number of messages that are send for each request.
                # Otherwise we might run out of memory for a lot of memory.
                MAX_ELMNTS_PER_REQ = 100
                events = itertools.islice(events, 0, MAX_ELMNTS_PER_REQ+1)
                events = list(events)
                if len(events)==MAX_ELMNTS_PER_REQ+1:
                    # There are more elements, but we are capping the result
                    for event in events[:-1]:
                        query_socket.send(event, zmq.SNDMORE)
                    query_socket.send(events[-1])
                else:
                    # Sending all events. Ie., we are not capping
                    for event in events:
                        query_socket.send(event, zmq.SNDMORE)
                    query_socket.send("END")


    incoming_socket.close()
    query_socket.close()
    streaming_socket.close()
    context.term()
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
