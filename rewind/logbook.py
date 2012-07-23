from __future__ import print_function
import argparse
import base64
import collections
import contextlib
import csv
import hashlib
import itertools
import logging
import os
import sqlite3
import sys
import types
import uuid

import zmq


logger = logging.getLogger(__name__)


class KeyValuePersister(collections.MutableMapping):
    """A persisted append-only MutableMapping implementation."""
    _delimiter = " "

    class InsertError(Exception):
        """Raised when trying to insert a malformed key or value."""
        pass

    def __init__(self, filename):
        self._filename = filename
        self._open()

    @staticmethod
    def _actually_populate_keyvals(filename):
        """Actually read the key/values from a file.

        Raises IOError if the file does not exit etcetera.
        """
        keyvals = {}
        with open(filename, 'rb') as f:
            for line in f:
                line = line.strip("\r\n")
                pieces = line.split(KeyValuePersister._delimiter)
                if len(pieces) >= 2:
                    key = pieces[0]
                    val = KeyValuePersister._delimiter.join(pieces[1:])
                    keyvals[key] = val
        return keyvals

    @staticmethod
    def _read_keyvals(filename):
        """Read the key/values if the file exists.
        
        returns -- a dictionary with key/values, or empty dictionary if the file
                   does not exist.
        """
        if os.path.exists(filename):
            return KeyValuePersister._actually_populate_keyvals(filename)
        else:
            return {}

    def _open(self):
        keyvals = self._read_keyvals(self._filename)

        rawfile = open(self._filename, 'ab')

        self._keyvals = keyvals
        self._file = rawfile

    def close(self):
        """Close the persister.

        Important to do to not have file descriptor leakages.
        """
        self._file.close()
        self._file = None

    def __delitem__(self, key):
        raise NotImplementedError('KeyValuePersister is append only.')

    def __getitem__(self, key):
        return self._keyvals[key]

    def __iter__(self):
        return iter(self._keyvals)

    def __len__(self):
        return len(self._keyvals)

    def __setitem__(self, key, val):
        if self._delimiter in key:
            msg = "Key contained delimiter: {0}".format(key)
            raise KeyValuePersister.InsertError(msg)
        if "\n" in key:
            msg = "Key must not contain any newline. It did: {0}"
            raise KeyValuePersister.InsertError(msg.format(key))
        if "\n" in val:
            msg = "Value must not contain any newline. It did: {0}"
            raise KeyValuePersister.InsertError(msg.format(val))
        if key in self._keyvals:
            self.close()
            oldval = self._keyvals[key]
            self._keyvals[key] = val
            try:
                with open(self._filename, 'wb') as f:
                    # Rewriting the whole file serially. Yes, it's a slow
                    # operation, but hey - it's an ascii file
                    for key, val in self._keyvals.iteritems():
                        f.write("{0}{1}{2}\n".format(key, self._delimiter, val))
            except Exception as e:
                self._keyvals[key] = oldval
                raise e
            finally:
                self._open()
        else:
            self._file.write("{0}{1}{2}\n".format(key, self._delimiter, val))
            self._keyvals[key] = val


class LogBookKeyError(KeyError):
    """Exception thrown if a requested key did not exist."""
    pass


class LogBookEventOrderError(IndexError):
    """Exception thrown if requested key is in wrong (chronological) order.

    That is if from-key was generated after to-key.
    """
    pass


class LogBookCorruptionError(Exception):
    """Exception raised when data seem corrupt."""
    pass


class EventStore(object):
    """Stores events and keeps track of their order.
    
    This class is here mostly for documentation. It shows which methods in
    general needs to be implemented by its supclasses.
    """
    class EventKeyAlreadyExistError(LogBookKeyError):
        """Raised when trying to add an event with a key already seen.

        While it is recommended that `EventStore`s raise this exception, they
        might not always. Maybe an `EventStore` can't hold most keys in memory
        etcetera.
        """
        pass

    class EventKeyDoesNotExistError(LogBookKeyError):
        """Raised when querying with a key that does not exist."""
        pass

    def add_event(self, key, event):
        """Adds an event with key to the store.
        
        Parameters:
        key   -- the key used to reference the event. The key must be a string.
        event -- the serialized event. The event must be a string or data.
        """
        raise NotImplementedError("Should be implemented by subclass.")

    def get_events(self, from_=None, to=None):
        """Query for events.

        Parameters:
        from_   -- receive all events seen later than the event with event id
                   `from_`. Note that this does not include `from_`.
        to      -- receive all events seen before the event with event id
                   `to`. Note that this also includes `to`.
        returns -- an iterable of (event id, eventdata) tuples.
        """
        raise NotImplementedError("Should be implemented by subclass.")

    def key_exists(self, key):
        """Check whether a key exists in the event store.

        Depending on the implementation of the event store, this method might
        not always look through the whole key space. In fact, it might not even
        look at all.
        """
        raise NotImplementedError("Should be implemented by subclass.")

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
        """See `EventStore.add_event(...)`."""
        if key in self.keys or key in self.events:
            raise EventStore.EventKeyAlreadyExistError(
                "The key already existed: {0}".format(key))
        self.keys.append(key)
        # Important no exceptions happen between these lines!
        self.events[key] = event

    def get_events(self, from_=None, to=None):
        """See `EventStore.add_get_events(...)`."""
        if from_ and (from_ not in self.keys or from_ not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: {0}".format(from_))
        if to and (to not in self.keys or to not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: {0}".format(to))
        
        # +1 here because we have already seen the event we are asking for
        fromindex = self.keys.index(from_)+1 if from_ else 0

        toindex = self.keys.index(to)+1 if to else len(self.events)
        if fromindex > toindex:
            msg = ("'From' index came after 'To'."
                   " Keys: ({0}, {1})"
                   " Indices: ({2}, {3})").format(from_, to, fromindex,
                                                  toindex)
            raise LogBookEventOrderError(msg)
        return ((key, self.events[key]) for key in self.keys[fromindex:toindex])

    def key_exists(self, key):
        """See `EventStore.key_exists(...)`."""
        return key in self.keys


class _SQLiteEventStore(EventStore):
    """Stores events in an sqlite database."""
    def __init__(self, path):
        fname = os.path.basename(path)
        checksum_persister = _get_checksum_persister(path)
        hasher = _initialize_hasher(path)
        if fname in checksum_persister and \
           checksum_persister[fname] != hasher.hexdigest():
            msg = "The file '{0}' had wrong md5 checksum.".format(path)
            raise LogBookCorruptionError(msg)

        # isolation_level=None => autocommit mode
        self.conn = sqlite3.connect(path, isolation_level=None)

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS events(
                eventid INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT UNIQUE,
                event BLOB
            );
        ''');

        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('PRAGMA integrity_check(1)')
            res = cursor.fetchone()
            status = res[0]
            assert status=='ok', \
                    "Integrity check failed when opening SQLite." \
                    " Actual status: {0}".format(status)

        self._path = path

    def add_event(self, key, event):
        """See `EventStore.add_event(...)`."""
        self.conn.execute('INSERT INTO events(uuid,event) VALUES (?,?)',
                     (key, event))

    def get_events(self, from_=None, to=None):
        """See `EventStore.get_events(...)`."""
        if from_ and not self.key_exists(from_):
            msg = 'from_={0}'.format(from_)
            raise EventStore.EventKeyDoesNotExistError(msg)
        if to and not self.key_exists(to):
            msg = 'to={0}'.format(to)
            raise EventStore.EventKeyDoesNotExistError(msg)

        # +1 below because we have already seen the event
        fromindex = self._get_eventid(from_)+1 if from_ else 0
        toindex = self._get_eventid(to) if to else None
        if from_ and to and fromindex > toindex:
            raise LogBookEventOrderError("'to' happened cronologically before"
                                         " 'from_'.")

        if toindex:
            sql = 'SELECT uuid, event FROM events WHERE eventid BETWEEN ? AND ?'
            params = (fromindex, toindex)
        else:
            sql = 'SELECT uuid, event FROM events WHERE eventid >= ?'
            params = (fromindex,)
        sql = sql + " ORDER BY eventid"
        return [(row[0], row[1]) for row in self.conn.execute(sql, params)]

    def _get_eventid(self, uuid):
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT eventid FROM events WHERE uuid=?', (uuid,))
            res = cursor.fetchone()
            return res[0]

    def key_exists(self, key):
        """See `EventStore.key_exists(...)`."""
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
            msg = 'There multiple event instances of: {0}'.format(key)
            raise RuntimeException(msg)

    def count(self):
        """Return the number of events in the db."""
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT COUNT(*) FROM events')
            res = cursor.fetchone()
            return res[0]

    def close(self):
        """Close the event store.
        
        Important to close to not have any file descriptor leakages.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

        fname = os.path.basename(self._path)
        checksum_persister = _get_checksum_persister(self._path)
        hasher = _initialize_hasher(self._path)
        with contextlib.closing(checksum_persister):
            checksum_persister[fname] = hasher.hexdigest()


def _hashfile(afile, hasher, blocksize=65536):
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)


def _initialize_hasher(path):
    hasher = hashlib.md5()
    if os.path.exists(path):
        with open(path) as f:
            _hashfile(f, hasher)
    return hasher


def _get_checksum_persister(path):
    directory = os.path.dirname(path)
    checksum_fname = os.path.join(directory, "checksums.md5")
    checksum_persister = KeyValuePersister(checksum_fname)
    return checksum_persister


class _LogEventStore(EventStore):
    def __init__(self, path):
        self._hasher = _initialize_hasher(path)

        fname = os.path.basename(path)
        checksum_persister = _get_checksum_persister(path)
        if fname in checksum_persister and \
           checksum_persister[fname] != self._hasher.hexdigest():
            msg = "The file '{0}' was had wrong md5.".format(path)
            raise LogBookCorruptionError(msg)
        
        self._path = path
        self._open()

    def _open(self):
        self.f = open(self._path, 'ab')

    def _close(self):
        if self.f:
            self.f.close()
            self.f = None

    def add_event(self, key, event):
        assert isinstance(key, str)
        assert isinstance(event, bytes)
        if all([char.isalnum() or char == '-' for char in key]):
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
        self._hasher.update(data)
        self.f.write(data)

    def _unsafe_get_events(self, from_, to):
        eventstrs = []
        with open(self._path) as f:
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
                eventstrs.append((key, base64.decodestring(eventstr)))
                if to and to == key:
                    found_to = True
                    break
            if to and not found_to:
                msg = 'from_={0}'.format(from_)
                raise EventStore.EventKeyDoesNotExistError(msg)

        return eventstrs

    def get_events(self, from_=None, to=None):
        """Get events.

        Does never throw LogBookEventOrderError because it is hard to detect
        from an append-only file.
        """
        assert from_ is None or isinstance(from_, str)
        assert to is None or isinstance(to, str)
        self._close()
        try:
            return self._unsafe_get_events(from_=from_, to=to)
        finally:
            self._open()

    def _unsafe_key_exists(self, needle):
        eventstrs = []
        with open(self._path) as f:
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
        assert isinstance(key, str)
        self._close()
        try:
            return self._unsafe_key_exists(key)
        finally:
            self._open()

    def close(self):
        """Persists a checksum and closes the file."""
        fname = os.path.basename(self._path)
        checksum_persister = _get_checksum_persister(self._path)
        with contextlib.closing(checksum_persister):
            checksum_persister[fname] = self._hasher.hexdigest()

        self._close()


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
        ignored_files = set(['checksums.md5'])
        files = [fname for fname in os.listdir(dirpath)
                 if fname not in ignored_files]

        identified_files = [fname for fname in files
                            if fname.startswith(prefix+'.')]
        not_identified_files = [fname for fname in files
                                if not fname.startswith(prefix+'.')]
        if not_identified_files:
            self.logger.warn("The following files could not be identified"
                             " (lacking prefix '%s'):", prefix)
            for not_identified_file in not_identified_files:
                self.logger.warn(' * %s', not_identified_file)

        if identified_files:
            nprefix = len(prefix) + 1
            batchnos = [file[nprefix:] for file in identified_files]
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
        """Rotate the files to disk.

        This is done by calling `store.close()` on each store, bumping the
        batchno and reopening the stores using their factories.
        """
        self.logger.info('Rotating data files. New batch number will be: %s',
                         self.batchno + 1)
        self.estore.close()
        self.estore = None
        self.batchno += 1
        self.estore = self._open_event_store()

    def add_event(self, key, event):
        """See `EventStore.add_event(...)`."""
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
        
        See `EventStore.add_event(...)` for details.
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
                for eventtuple in estore.get_events(from_in_batch, to_in_batch):
                    yield eventtuple

    def key_exists(self, key):
        """Checks whether the key exists in the current event store."""
        return self.estore.key_exists(key)

    def close(self):
        """Close the event store."""
        self.estore.close()
        self.estore = None


class SyncedRotationEventStores(EventStore):
    """Wraps multiple `RotatedEventStore` event stores.
    
    Rotation is done at the same time for all event stores to make sure they are
    kept in sync.
    """
    def __init__(self, events_per_batch=25000):
        """Construct a persisted event store that is stored on disk.
        
        Parameters:
        events_per_batch -- number of events stored in a batch before rotating
                            the files. Defaults to 25000. That number is
                            arbitrary and should probably be configures so that
                            files do not grow out of proportion.
        """
        assert isinstance(events_per_batch, int), \
                "Events per batch must be integer."
        assert events_per_batch > 0, "Events per batch must be positive"
        self.events_per_batch = events_per_batch
        self.count = 0
        self.stores = []

    def add_rotated_store(self, rotated_store):
        """Add a `RotatedEventStore` that shall be rotated with the others."""
        assert self.count == 0, \
                "Must not have written before adding additional estores"
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
        """Close all underlying stores."""
        self._close_event_stores()

    def add_event(self, key, event):
        """See `EventStore.add_event(...)`."""
        if self.key_exists(key):
            # This check might actually also be done further up in the chain
            # (read: _SQLiteEventStore). Could potentially be removed if it
            # requires a lot of processor cycles.
            msg = "The key already existed: {0}".format(key)
            raise EventStore.EventKeyAlreadyExistError(msg)

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
        """Query events.
        
        The events are queried from the event store that was added first using
        `add_rotated_store(...)`.

        See `EventStore.get_event(...)`."""
        return self.stores[0].get_events(from_, to)


class IdGenerator:
    """Generates unique strings."""
    def __init__(self, key_exists=lambda key: False):
        self.key_exists = key_exists

    def _propose_new_key(self):
        return str(uuid.uuid4())

    def generate(self):
        """Generate a new string and return it."""
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

        newid = self.id_generator.generate()
        
        # Make sure newid is not part of our request vocabulary
        assert newid != "QUERY", \
                "Generated ID must not be part of req/rep vocabulary."
        assert not newid.startswith("ERROR"), \
                "Generated ID must not be part of req/rep vocabulary."

        # Important this is done before forwarding to the streaming socket
        self.eventstore.add_event(newid, eventstr)

        self.streaming_socket.send(newid, zmq.SNDMORE)
        self.streaming_socket.send(eventstr)

        return True

    def _handle_query(self):
        """Handle an event query."""
        requesttype = self.query_socket.recv()
        if requesttype == "QUERY":
            assert self.query_socket.getsockopt(zmq.RCVMORE)
            fro = self.query_socket.recv()
            assert self.query_socket.getsockopt(zmq.RCVMORE)
            to = self.query_socket.recv()
            assert not self.query_socket.getsockopt(zmq.RCVMORE)

            logging.debug("Incoming query: (from, to)=(%s, %s)", fro, to)

            try:
                events = self.eventstore.get_events(fro if fro else None,
                                                    to if to else None)
            except EventStore.EventKeyDoesNotExistError as e:
                logger.exception("A client requested a key that does not"
                                 " exist:")
                self.query_socket.send("ERROR Key did not exist")
                return True

            # Since we are using ZeroMQ enveloping we want to cap the
            # maximum number of messages that are send for each request.
            # Otherwise we might run out of memory for a lot of events.
            MAX_ELMNTS_PER_REQ = 100

            events = itertools.islice(events, 0, MAX_ELMNTS_PER_REQ+1)
            events = list(events)
            if len(events)==MAX_ELMNTS_PER_REQ+1:
                # There are more elements, but we are capping the result
                for eventid, eventdata in events[:-1]:
                    self.query_socket.send(eventid, zmq.SNDMORE)
                    self.query_socket.send(eventdata, zmq.SNDMORE)
                lasteventid, lasteventdata = events[-1]
                self.query_socket.send(lasteventid, zmq.SNDMORE)
                self.query_socket.send(lasteventdata)
            else:
                # Sending all events. Ie., we are not capping
                for eventid, eventdata in events:
                    self.query_socket.send(eventid, zmq.SNDMORE)
                    self.query_socket.send(eventdata, zmq.SNDMORE)
                self.query_socket.send("END")
        else:
            logging.warn("Could not identify request type: %s", requesttype)
            self.query_socket.send("ERROR Unknown request type")

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
        dbdir = os.path.join(args.datadir, 'db')
        def db_creator(filename):
            return _SQLiteEventStore(filename)
        rotated_db_estore = RotatedEventStore(db_creator, dbdir, 'sqlite')

        logdir = os.path.join(args.datadir, 'appendlog')
        def log_creator(filename):
            return _LogEventStore(filename)
        rotated_log_estore = RotatedEventStore(log_creator, logdir,
                                               'appendlog')

        EVENTS_PER_BATCH = 30000
        eventstore = SyncedRotationEventStores(EVENTS_PER_BATCH)

        # Important DB event store is added first. Otherwise, fast event
        # querying will not be enabled.
        eventstore.add_rotated_store(rotated_db_estore)
        eventstore.add_rotated_store(rotated_log_estore)

        # TODO: Make sure event stores are correctly mirrored
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
            print(errmsg)
            return 2

    exitcode = run(args)
    return exitcode
