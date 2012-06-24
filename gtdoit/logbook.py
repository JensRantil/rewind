import sys
import argparse
import sys
import logging
import uuid
import itertools
import contextlib
import abc
import sqlite3
import base64
import os

import zmq

import gtdoit.messages.events_pb2 as events_pb2
import gtdoit.messages.eventhandling_pb2 as eventhandling_pb2


logger = logging.getLogger(__name__)


class LogBookKeyError(KeyError):
    """Exception thrown if a requested key did not exist."""
    pass


class LogBookEventOrderError(IndexError):
    """Exception thrown if requested key is in wrong (chronological) order.

    That is if from-key was generated after to-key.
    """
    pass


class EventStore(object):
    """Stores events and keeps track of their order.
    
    Abstract class.
    """
    __metaclass__ = abc.ABCMeta

    class EventKeyAlreadyExistError(LogBookKeyError):
        pass

    class EventKeyDoesNotExistError(LogBookKeyError):
        pass

    @abc.abstractmethod
    def add_event(self, key, event):
        pass

    @abc.abstractmethod
    def get_events(self, from_=None, to=None):
        pass

    @abc.abstractmethod
    def key_exists(self, key):
        pass

    def close(self):
        """Close the store.

        May be overridden if necessary.
        """
        pass


class InMemoryEventStore(EventStore):
    """Stores events in-memory and keeps track of their order."""
    def __init__(self):
        self.keys = []
        self.events = {}

    def add_event(self, key, event):
        if key in self.keys or key in self.events:
            raise EventStore.EventKeyAlreadyExistError(
                "The key already existed: %s" % key)
        self.keys.append(key)
        # Important no exceptions happen between these lines!
        self.events[key] = event

    def get_events(self, from_=None, to=None):
        if from_ and (from_ not in self.keys or from_ not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: %s" % from_)
        if to and (to not in self.keys or to not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: %s" % to)
        
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

EventStore.register(InMemoryEventStore)


class _SQLiteEventStore(EventStore):
    """Stores events in an sqlite database."""
    def __init__(self, path):
        # isolation_level=None => autocommit mode
        self.conn = sqlite3.connect(path, isolation_level=None)

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS events(
                eventid INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT UNIQUE,
                event BLOB
            );
        ''');

    def add_event(self, key, event):
        self.conn.execute('INSERT INTO events(uuid,event) VALUES (?,?)',
                     (key, event))

    def get_events(self, from_=None, to=None):
        if from_ and not self.key_exists(from_):
            raise EventStore.EventKeyDoesNotExistError('from_=%s' % from_)
        if to and not self.key_exists(to):
            raise EventStore.EventKeyDoesNotExistError('to=%s' % to)

        # +1 below because we have already seen the event
        fromindex = self._get_eventid(from_)+1 if from_ else 0
        toindex = self._get_eventid(to) if to else None
        if from_ and to and fromindex > toindex:
            raise LogBookEventOrderError("'to' happened cronologically before"
                                         " 'from_'.")

        if toindex:
            sql = 'SELECT event FROM events WHERE eventid BETWEEN ? AND ?'
            params = (fromindex, toindex)
        else:
            sql = 'SELECT event FROM events WHERE eventid >= ?'
            params = (fromindex,)
        sql = sql + " ORDER BY eventid"
        return [row[0] for row in self.conn.execute(sql, params)]

    def _get_eventid(self, uuid):
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT eventid FROM events WHERE uuid=?', (uuid,))
            res = cursor.fetchone()
            return res[0]

    def key_exists(self, key):
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT COUNT(*) FROM events WHERE uuid=?', (key,))
            res = cursor.fetchone()
            count = res[0]
        if count == 0:
            return False
        elif count == 1:
            return True
        else:
            raise RuntimeException('There multiple event instances of: %s' %
                                   key)

    def count(self):
        """Return the number of events in the db."""
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT COUNT(*) FROM events')
            res = cursor.fetchone()
            return res[0]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

EventStore.register(_SQLiteEventStore)


class _LogEventStore(EventStore):
    def __init__(self, path):
        self.path = path
        self._open()

    def _open(self):
        self.f = open(self.path, 'ab')

    def _close(self):
        if self.f:
            self.f.close()
            self.f = None

    def add_event(self, key, event):
        if all([char.isalnum() or char=='-' for char in key]):
            safe_key = key
        else:
            raise ValueError("Key must be alphanumeric or a dash (-):"
                             " {0}".format(key))
        safe_event = base64.encodestring(event).strip()
        if not all([char.isalnum() or char=='=' for char in safe_event]):
            raise ValueError("Safe event string must be alphanumeric or '=':"
                             " {0}".format(safe_event))
        data = "{0}\t{1}\n".format(safe_key, safe_event)
        
        # Important to make a single atomic write here
        self.f.write(data)

    def _unsafe_get_events(self, from_, to):
        eventstrs = []
        with open(self.path) as f:
            # Find events from 'from_' (or start if not given, that is)
            if from_:
                for line in f:
                    key, eventstr = line.strip().split("\t")
                    if key == from_:
                        break

            # Continue until 'to' (if given, that is)
            found_to = False
            for line in f:
                key, eventstr = line.strip().split("\t")
                eventstrs.append(eventstr)
                if to and to == key:
                    found_to = True
                    break
            if to and not found_to:
                raise EventStore.EventKeyDoesNotExistError('from_=%s' % from_)

        return [base64.decodestring(eventstr) for eventstr in eventstrs]

    def get_events(self, from_=None, to=None):
        """Get events.

        Does never throw LogBookEventOrderError because it is hard to detect
        from an append-only file.
        """
        self._close()
        try:
            return self._unsafe_get_events(from_=from_, to=to)
        finally:
            self._open()

    def _unsafe_key_exists(self, needle):
        eventstrs = []
        with open(self.path) as f:
            # Find events from 'from_' (or start if not given, that is)
            for line in f:
                key, eventstr = line.strip().split("\t")
                if key == needle:
                    return True
        return False

    def key_exists(self, key):
        """Checks for key existence.

        Makes a linear search and is very slow.
        """
        self._close()
        try:
            return self._unsafe_key_exists(key)
        finally:
            self._open()

    def close(self):
        self._close()

EventStore.register(_LogEventStore)


class RotatedEventStore(EventStore):
    """An event store that stores events in rotated files."""
    def __init__(self, estore_factory, dirpath, prefix):
        # These needs to be specified before calling self._determine_batchno()
        self.dirpath = dirpath
        self.prefix = prefix
        self.logger = logger.getChild(prefix)

        if not os.path.exists(dirpath):
            self.logger.info('Creating rotated eventstore directory: %s',
                             dirpath)
            os.mkdir(dirpath)
            batchno = 0
        else:
            batchno = self._determine_batchno()

        self.estore_factory = estore_factory
        self.batchno = batchno

        self.estore = self._open_event_store()

    def _determine_batchno(self):
        dirpath = self.dirpath
        prefix = self.prefix
        files = os.listdir(dirpath)

        identified_files = [fname for fname in files
                            if fname.startswith(prefix+'.')]
        not_identified_files = [fname for fname in files
                                if not fname.startswith(prefix+'.')]
        if not_identified_files:
            self.logger.warn("The following files could not be identified"
                             "(lacking prefix '%s'):", prefix)
            for not_identified_file in not_identified_files:
                logger.warn(' * %s', not_identified_file)

        if files:
            nprefix = len(prefix) + 1
            batchnos = [file[nprefix:] for file in files]
            last_batch = max([int(batchno) for batchno in batchnos])
        else:
            last_batch = 0

        return last_batch

    def _open_event_store(self, batchno=None):
        batchno = self.batchno if batchno is None else batchno
        fname = self._construct_filename(batchno)
        return self.estore_factory(fname)

    def _construct_filename(self, batchno):
        """Construct a filename for a database.

        Parameters:
        batchno -- batch number for the rotated database.
        """
        return os.path.join(self.dirpath,
                            "{0}.{1}".format(self.prefix, batchno))

    def rotate(self):
        self.logger.info('Rotating data files. New batch number will be: %s',
                    self.batchno + 1)
        self.estore.close()
        self.estore = None
        self.batchno += 1
        self.estore = self._open_event_store()

    def add_event(self, key, event):
        self.estore.add_event(key, event)

    def _find_batch_containing_event(self, uuid):
        """Find the batch number that contains a certain event.

        Parameters:
        uuid    -- the event uuid to search for.
        returns -- a batch number, or None if not found.
        """
        if self.estore.key_exists(uuid):
            # Reusing already opened DB if possible
            return self.batchno
        else:
            for batchno in range(self.batchno-1, -1, -1):
                # Iterating backwards here because we are more likely to find
                # the event in an later archive, than earlier.
                db = self._open_event_store(batchno)
                with contextlib.closing(db):
                    if db.key_exists(uuid):
                        return batchno
        return None

    def get_events(self, from_=None, to=None):
        """Query events.

        It also queries older event archives until it finds the event UUIDs
        necessary.
        """
        if from_:
            frombatchno = self._find_batch_containing_event(from_)
            if frombatchno is None:
                raise EventKeyDoesNotExistError('from_={0}'.format(from_))
        else:
            frombatchno = 0
        if to:
            tobatchno = self._find_batch_containing_event(to)
            if tobatchno is None:
                raise EventStore.EventKeyDoesNotExistError('to={0}'.format(to))
        else:
            tobatchno = self.batchno

        batchno_range = range(frombatchno, tobatchno+1)
        nbatches = len(batchno_range)
        if nbatches==1:
            event_ranges = [(from_, to)]
        else:
            event_ranges = itertools.chain([(from_, None)],
                                           itertools.repeat((None,None),
                                                            nbatches-2),
                                           [(None, to)])
        for batchno, (from_in_batch, to_in_batch) in zip(batchno_range,
                                                         event_ranges):
            estore = self._open_event_store(batchno)
            with contextlib.closing(estore):
                for event in estore.get_events(from_in_batch, to_in_batch):
                    yield event

    def key_exists(self, key):
        """Checks whether the key exists in the current event store."""
        return self.estore.key_exists(key)

EventStore.register(RotatedEventStore)


class RotationEventStore(EventStore):
    """Wraps multiple `RotatedEventStore` event stores.
    
    Rotation is done at the same time for all event stores to make sure they are
    kept in sync.
    """
    class IntegrityError(RuntimeError):
        pass

    def __init__(self, events_per_batch=25000):
        """Construct a persisted event store that is stored on disk.
        
        Parameters:
        events_per_batch -- number of events stored in a batch before rotating
                            the files.
        """
        self.events_per_batch = events_per_batch
        self.count = 0
        self.stores = []

        # TODO: Test that 'md5sum' exists

    def add_rotated_store(self, rotated_store):
        self.stores.append(rotated_store)

    def _rotate_files_if_needed(self):
        if self.count >= self.events_per_batch:
            if logger.isEnabledFor(logging.DEBUG):
                msg = 'Rotating because number of events(=%s) exceeds %s.'
                logger.debug(msg, self.count, self.events_per_batch)
            self._rotate_files()

    def _rotate_files(self):
        for estore in self.stores:
            estore.rotate()
        self.count = 0

    def _close_event_stores(self):
        """Signal that we are done with the backend event stores."""
        for store in self.stores:
            store.close()
        self.stores = []

    def close(self):
        self._close_event_stores()

    def check_integrity(self):
        """Check the filesystem integrity of the files.
        
        Static class function to minimize state.

        TODO: Test.
        """
        RotationEventStore._test_log_filename_format(logpath)
        RotationEventStore._test_db_filename_format(dbpath)

        # Simply shortened constants
        dbprefix = RotationEventStore.DATABASE_PREFIX
        logprefix = RotationEventStore.LOG_PREFIX

        dbfiles = os.listdir(dbpath)
        logfiles = os.listdir(logpath)

        for batchno in range(max(len(dbfiles), len(logfiles))):
            dbfile = os.path.join(dbpath,
                                  "{0}{1}".format(dbprefix, batchno))
            logfile = os.path.join(logpath,
                                   "{0}{1}".format(logprefix, batchno))

            # Tests
            dbfile_exists = os.path.exists(dbfile)
            logfile_exists = os.path.exists(logfile)
            if not dbfile_exists:
                logger.warn("Integrity issue. Missing file: %s", dbfile)
            if not logfile_exists:
                logger.warn("Integrity issue. Missing file: %s", logfile)
            if not dbfile_exists:
                RotationEventStore.IntegrityError("Missing file: %s",
                                                   dbfile)
            if not logfile_exists:
                RotationEventStore.IntegrityError("Missing file: %s",
                                                   logfile)
            if not os.path.isfile(dbfile):
                RotationEventStore.IntegrityError("Not regular file: %s",
                                                   dbfile)
            if not os.path.isfile(logfile):
                RotationEventStore.IntegrityError("Not regular file: %s",
                                                   logfile)

            # TODO: Recreate database from log if it seems corrupt.
            # TODO: Fail only if database exists, but log does not.
            
        # TODO: Write code that verifies MD5 for logs

    def add_event(self, key, event):
        if self.key_exists(key):
            # This check might actually also be done further up in the chain
            # (read: _SQLiteEventStore). Could potentially be removed if it
            # requires a lot of processor cycles.
            raise EventKeyAlreadyExistError("The key already existed: %s" % key)

        self._rotate_files_if_needed()

        # Since I guess _LogEventStore is less mature codewise than
        # _SQLiteEventStore I am writing to that log file first. If something
        # fails we are not writing to _SQLiteEventStore.
        for store in self.stores:
            store.add_event(key, event)
        self.count += 1

    def key_exists(self, key):
        """Checks for event key existence.

        This check is actually only done to the last batch to make is really
        fast. Therefor, it's mostly to make sure we have a sane UUID generator.
        """
        return self.stores[0].key_exists(key)

    def get_events(self, from_=None, to=None):
        return self.stores[0].get_events(from_, to)

EventStore.register(RotationEventStore)


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
    if args.datadir:
        eventstore = RotationEventStore(args.datadir)
    else:
        logger.warn("Using InMemoryEventStore. Events are not persisted."
                    " See --datadir parameter for further info.")
        eventstore = InMemoryEventStore()

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


def main(argv=None):
    """Entry point for the logbook.

    Parses input and calls run() for the real work.

    Parameters:
    argv -- sys.argv arguments. Can be set for testing purposes.
    """
    parser = argparse.ArgumentParser(
        description='Event storage and event proxy.'
    )
    parser.add_argument('--exit-codeword', metavar="MSG", dest="exit_message",
                        help="An incoming message that makes the logbook quit."
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
                              help='the connect address for streaming of events',
                              action='append',
                              dest='streaming_connect_endpoints')

    args = argv if argv is not None else sys.argv[1:]
    args = parser.parse_args(args)

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
    sys.exit(exitcode)
