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

"""Different event stores.

Every event stores is derived from `EventStore`.

"""

import base64
import collections
import contextlib
import hashlib
import itertools
import logging
import os
import sqlite3


_logger = logging.getLogger(__name__)


# Utility functions and classes used by event stores

def _get_checksum_persister(path):
    directory = os.path.dirname(path)
    checksum_fname = os.path.join(directory, "checksums.md5")
    checksum_persister = _KeyValuePersister(checksum_fname)
    return checksum_persister


def _hashfile(afile, hasher, blocksize=65536):
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)


class _KeyValuePersister(collections.MutableMapping):

    """A persisted append-only MutableMapping implementation."""

    _delimiter = " "

    class InsertError(Exception):
        """Raised when trying to insert a malformed key or value."""
        pass

    def __init__(self, filename):
        assert isinstance(filename, str)
        self._filename = filename
        self._open()

    @staticmethod
    def _actually_populate_keyvals(filename):
        """Return a dictionary containing the key/values read from filename.

        Raises IOError if the file does not exit etcetera.

        """
        assert isinstance(filename, str)
        keyvals = {}
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip("\r\n")
                pieces = line.split(_KeyValuePersister._delimiter)
                if len(pieces) >= 2:
                    key = pieces[0]
                    val = _KeyValuePersister._delimiter.join(pieces[1:])
                    keyvals[key] = val
        return keyvals

    @staticmethod
    def _read_keyvals(filename):
        """Read the key/values if the file exists.

        returns -- a dictionary with key/values, or empty dictionary if the
                   file does not exist.

        """
        assert isinstance(filename, str)
        if os.path.exists(filename):
            return _KeyValuePersister._actually_populate_keyvals(filename)
        else:
            return {}

    def _open(self):
        keyvals = self._read_keyvals(self._filename)

        rawfile = open(self._filename, 'a')

        self._keyvals = keyvals
        self._file = rawfile

    def close(self):
        """Close the persister.

        Important to do to not have file descriptor leakages.

        """
        self._file.close()
        self._file = None

    def __delitem__(self, key):
        raise NotImplementedError('_KeyValuePersister is append only.')

    def __getitem__(self, key):
        assert isinstance(key, str)
        return self._keyvals[key]

    def __iter__(self):
        return iter(self._keyvals)

    def __len__(self):
        return len(self._keyvals)

    def __setitem__(self, key, val):
        assert isinstance(key, str)
        assert isinstance(val, str)
        if self._delimiter in key:
            msg = "Key contained delimiter: {0}".format(key)
            raise _KeyValuePersister.InsertError(msg)
        if "\n" in key:
            msg = "Key must not contain any newline. It did: {0}"
            raise _KeyValuePersister.InsertError(msg.format(key))
        if "\n" in val:
            msg = "Value must not contain any newline. It did: {0}"
            raise _KeyValuePersister.InsertError(msg.format(val))
        if key in self._keyvals:
            self.close()
            oldval = self._keyvals[key]
            self._keyvals[key] = val
            try:
                with open(self._filename, 'w') as f:
                    # Rewriting the whole file serially. Yes, it's a slow
                    # operation, but hey - it's an ascii file
                    for key, val in self._keyvals.items():
                        f.write(key)
                        f.write(self._delimiter)
                        f.write(val)
                        f.write('\n')
            except Exception as e:
                self._keyvals[key] = oldval
                raise e
            finally:
                self._open()
        else:
            self._file.write(key)
            self._file.write(self._delimiter)
            self._file.write(val)
            self._file.write('\n')
            self._keyvals[key] = val


def _initialize_hasher(path):
    hasher = hashlib.md5()
    if os.path.exists(path):
        with open(path, 'rb') as f:
            _hashfile(f, hasher)
    return hasher


# Errors thrown by event stores

class EventStoreError(KeyError):

    """Exception thrown if a requested key did not exist."""

    pass


class EventOrderError(IndexError):

    """Exception thrown if requested key is in wrong (chronological) order.

    That is if from-key was generated after to-key.

    """

    pass


class CorruptionError(Exception):

    """Exception raised when data seem corrupt."""

    pass


# Event stores

class EventStore(object):

    """Stores events and keeps track of their order.

    This class is here mostly for documentation. It shows which methods in
    general needs to be implemented by its supclasses.

    """

    class EventKeyAlreadyExistError(EventStoreError):
        """Raised when trying to add an event with a key already seen.

        While it is recommended that `EventStore`s raise this exception, they
        might not always. Maybe an `EventStore` can't hold most keys in memory
        etcetera.

        """
        pass

    class EventKeyDoesNotExistError(EventStoreError):
        """Raised when querying with a key that does not exist."""
        pass

    def add_event(self, key, event):
        """Add event and corresponding key to the store.

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
        """Add an event and its corresponding key to the store."""
        if key in self.keys or key in self.events:
            raise EventStore.EventKeyAlreadyExistError(
                "The key already existed: {0}".format(key))
        self.keys.append(key)
        # Important no exceptions happen between these lines!
        self.events[key] = event

    def get_events(self, from_=None, to=None):
        """Query a slice of the events.

        Events are always returned in the order the were added.

        Parameters:
        from_   -- if not None, return only events added after the event with
                   id `from_`. If None, return from the start of history.
        to      -- if not None, return only events added before, and
                   including, the event with event id `to`. If None, return up
                   to, and including, the last added event.

        returns -- an iterable of (event id, eventdata) tuples.

        """
        if from_ and (from_ not in self.keys or from_ not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: {0}".format(from_))
        if to and (to not in self.keys or to not in self.events):
            raise EventStore.EventKeyDoesNotExistError(
                "Could not find the from_ key: {0}".format(to))

        # +1 here because we have already seen the event we are asking for
        fromindex = self.keys.index(from_) + 1 if from_ else 0

        toindex = self.keys.index(to) + 1 if to else len(self.events)
        if fromindex > toindex:
            msg = ("'From' index came after 'To'."
                   " Keys: ({0}, {1})"
                   " Indices: ({2}, {3})").format(from_, to, fromindex,
                                                  toindex)
            raise EventOrderError(msg)
        return ((key, self.events[key])
                for key in self.keys[fromindex:toindex])

    def key_exists(self, key):
        """Check whether a key exists in the event store.

        Returns True if it does, False otherwise.

        """
        return key in self.keys


class SQLiteEventStore(EventStore):

    """Stores events in an sqlite database."""

    def __init__(self, path):
        fname = os.path.basename(path)
        checksum_persister = _get_checksum_persister(path)
        hasher = _initialize_hasher(path)
        if (fname in checksum_persister and
                checksum_persister[fname] != hasher.hexdigest()):
            msg = "The file '{0}' had wrong md5 checksum.".format(path)
            raise CorruptionError(msg)

        # isolation_level=None => autocommit mode
        self.conn = sqlite3.connect(path, isolation_level=None)

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS events(
                eventid INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT UNIQUE,
                event BLOB
            );
        ''')

        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('PRAGMA integrity_check(1)')
            res = cursor.fetchone()
            status = res[0]
            assert status == 'ok', \
                ("Integrity check failed when opening SQLite."
                 " Actual status: {0}".format(status))

        self._path = path

    def add_event(self, key, event):
        """Add an event and its corresponding key to the store."""
        assert isinstance(key, str)
        assert isinstance(event, bytes)
        self.conn.execute('INSERT INTO events(uuid,event) VALUES (?,?)',
                          (key, event.decode('utf-8')))

    def get_events(self, from_=None, to=None):
        """Query a slice of the events.

        Events are always returned in the order the were added.

        Parameters:
        from_   -- if not None, return only events added after the event with
                   id `from_`. If None, return from the start of history.
        to      -- if not None, return only events added before, and
                   including, the event with event id `to`. If None, return up
                   to, and including, the last added event.

        returns -- an iterable of (event id, eventdata) tuples.

        """
        assert from_ is None or isinstance(from_, str)
        assert to is None or isinstance(to, str)
        if from_ and not self.key_exists(from_):
            msg = 'from_={0}'.format(from_)
            raise EventStore.EventKeyDoesNotExistError(msg)
        if to and not self.key_exists(to):
            msg = 'to={0}'.format(to)
            raise EventStore.EventKeyDoesNotExistError(msg)

        # +1 below because we have already seen the event
        fromindex = self._get_eventid(from_) + 1 if from_ else 0
        toindex = self._get_eventid(to) if to else None
        if from_ and to and fromindex > toindex:
            raise EventOrderError("'to' happened cronologically before"
                                  " 'from_'.")

        if toindex:
            sql = ('SELECT uuid, event FROM events '
                   'WHERE eventid BETWEEN ? AND ?')
            params = (fromindex, toindex)
        else:
            sql = 'SELECT uuid, event FROM events WHERE eventid >= ?'
            params = (fromindex,)
        sql = sql + " ORDER BY eventid"
        return [(row[0], row[1].encode('utf-8'))
                for row in self.conn.execute(sql, params)]

    def _get_eventid(self, uuid):
        assert isinstance(uuid, str)
        cursor = self.conn.cursor()
        with contextlib.closing(cursor):
            cursor.execute('SELECT eventid FROM events WHERE uuid=?', (uuid,))
            res = cursor.fetchone()
            return res[0]

    def key_exists(self, key):
        """Check whether a key exists in the event store.

        Returns True if it does, False otherwise.

        """
        assert isinstance(key, str)
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


class LogEventStore(EventStore):

    """File-backed append only ascii event store.

    Each event is persisted, one line per event, to a file.

    """

    def __init__(self, path):
        self._hasher = _initialize_hasher(path)

        fname = os.path.basename(path)
        checksum_persister = _get_checksum_persister(path)
        if (fname in checksum_persister and
                checksum_persister[fname] != self._hasher.hexdigest()):
            msg = "The file '{0}' was had wrong md5.".format(path)
            raise CorruptionError(msg)

        self._path = path
        self._open()

    def _open(self):
        self.f = open(self._path, 'at')

    def _close(self):
        if self.f:
            self.f.close()
            self.f = None

    def add_event(self, key, event):
        """Add an event and its corresponding key to the store."""
        assert isinstance(key, str)
        assert isinstance(event, bytes)
        if all([char.isalnum() or char == '-' for char in key]):
            safe_key = key
        else:
            raise ValueError("Key must be alphanumeric or a dash (-):"
                             " {0}".format(key))
        safe_event = base64.encodestring(event).decode().strip()
        data = "{0}\t{1}\n".format(safe_key, safe_event)

        # Important to make a single atomic write here
        self._hasher.update(data.encode())
        self.f.write(data)

    def _unsafe_get_events(self, from_, to):
        eventstrs = []
        with open(self._path) as f:
            # Find events from 'from_' (or start if not given, that is)
            if from_:
                for line in f:
                    key, eventstr = line.rstrip('\n\r').split("\t")
                    if key == from_:
                        break

            # Continue until 'to' (if given, that is)
            found_to = False
            for line in f:
                key, eventstr = line.rstrip('\n\r').split("\t")
                decodedevstr = base64.decodestring(eventstr.encode())
                eventstrs.append((key, decodedevstr))
                if to and to == key:
                    found_to = True
                    break
            if to and not found_to:
                msg = 'from_={0}'.format(from_)
                raise EventStore.EventKeyDoesNotExistError(msg)

        return eventstrs

    def get_events(self, from_=None, to=None):
        """Query a slice of the events.

        Events are always returned in the order the were added.

        Does never throw EventOrderError because it is hard to detect
        from an append-only file.

        Parameters:
        from_   -- if not None, return only events added after the event with
                   id `from_`. If None, return from the start of history.
        to      -- if not None, return only events added before, and
                   including, the event with event id `to`. If None, return up
                   to, and including, the last added event.

        returns -- an iterable of (event id, eventdata) tuples.

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
        """Check if key has previously been added to this store.

        This function makes a linear search through the log file and is very
        slow.

        Returns True if the event has previously been added, False otherwise.

        """
        assert isinstance(key, str)
        self._close()
        try:
            return self._unsafe_key_exists(key)
        finally:
            self._open()

    def close(self):
        """Persist a checksum and close the file."""
        fname = os.path.basename(self._path)
        checksum_persister = _get_checksum_persister(self._path)
        with contextlib.closing(checksum_persister):
            checksum_persister[fname] = self._hasher.hexdigest()

        self._close()


class RotatedEventStore(EventStore):

    """An event store that stores events in rotated files.

    Calls to `EventStore` specific methods are simply proxied to the event
    store created by the factory specified as an argument to the constructor.

    """

    def __init__(self, estore_factory, dirpath, prefix):
        # These needs to be specified before calling self._determine_batchno()
        self.dirpath = dirpath
        self.prefix = prefix
        self._logger = _logger.getChild(prefix)

        if not os.path.exists(dirpath):
            self._logger.info('Creating rotated eventstore directory: %s',
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
                            if fname.startswith(prefix + '.')]
        not_identified_files = [fname for fname in files
                                if not fname.startswith(prefix + '.')]
        if not_identified_files:
            self._logger.warn("The following files could not be identified"
                              " (lacking prefix '%s'):", prefix)
            for not_identified_file in not_identified_files:
                self._logger.warn(' * %s', not_identified_file)

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

        Returns the constructed path as a string.

        """
        return os.path.join(self.dirpath,
                            "{0}.{1}".format(self.prefix, batchno))

    def rotate(self):
        """Rotate the files to disk.

        This is done by calling `store.close()` on each store, bumping the
        batchno and reopening the stores using their factories.

        """
        self._logger.info('Rotating data files. New batch number will be: %s',
                          self.batchno + 1)
        self.estore.close()
        self.estore = None
        self.batchno += 1
        self.estore = self._open_event_store()

    def add_event(self, key, event):
        """Add an event and its corresponding key to the store."""
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
            for batchno in range(self.batchno - 1, -1, -1):
                # Iterating backwards here because we are more likely to find
                # the event in an later archive, than earlier.
                db = self._open_event_store(batchno)
                with contextlib.closing(db):
                    if db.key_exists(uuid):
                        return batchno
        return None

    def get_events(self, from_=None, to=None):
        """Query a slice of the events.

        Events are always returned in the order the were added.

        It also queries older event archives until it finds the event UUIDs
        necessary.

        Parameters:
        from_   -- if not None, return only events added after the event with
                   id `from_`. If None, return from the start of history.
        to      -- if not None, return only events added before, and
                   including, the event with event id `to`. If None, return up
                   to, and including, the last added event.

        returns -- an iterable of (event id, eventdata) tuples.

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

        batchno_range = range(frombatchno, tobatchno + 1)
        nbatches = len(batchno_range)
        if nbatches == 1:
            event_ranges = [(from_, to)]
        else:
            event_ranges = itertools.chain([(from_, None)],
                                           itertools.repeat((None, None),
                                                            nbatches - 2),
                                           [(None, to)])
        for batchno, (from_in_batch, to_in_batch) in zip(batchno_range,
                                                         event_ranges):
            estore = self._open_event_store(batchno)
            with contextlib.closing(estore):
                for eventtuple in estore.get_events(from_in_batch,
                                                    to_in_batch):
                    yield eventtuple

    def key_exists(self, key):
        """Check whether key exists has been added to this event store.

        Returns True if it has, False otherwise.

        """
        return self.estore.key_exists(key)

    def close(self):
        """Close the event store."""
        self.estore.close()
        self.estore = None


class SyncedRotationEventStores(EventStore):

    """Wraps multiple `RotatedEventStore` event stores.

    Rotation is done at the same time for all event stores to make sure they
    are kept in sync.

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
            if _logger.isEnabledFor(logging.DEBUG):
                msg = 'Rotating because number of events(=%s) exceeds %s.'
                _logger.debug(msg, self.count, self.events_per_batch)
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
        """Add an event and its corresponding key to the store."""
        if self.key_exists(key):
            # This check might actually also be done further up in the chain
            # (read: SQLiteEventStore). Could potentially be removed if it
            # requires a lot of processor cycles.
            msg = "The key already existed: {0}".format(key)
            raise EventStore.EventKeyAlreadyExistError(msg)

        self._rotate_files_if_needed()

        # Since I guess LogEventStore is less mature codewise than
        # SQLiteEventStore I am writing to that log file first. If something
        # fails we are not writing to SQLiteEventStore.
        for store in self.stores:
            store.add_event(key, event)
        self.count += 1

    def key_exists(self, key):
        """Check if key has has previously been added to this event store..

        This check is actually only done to the last batch to make is really
        fast. Therefor, it's mostly to make sure we have a sane UUID generator.

        Return True if it has been added before, False otherwise.

        """
        return self.stores[0].key_exists(key)

    def get_events(self, from_=None, to=None):
        """Query a slice of the events.

        Events are always returned in the order the were added.

        The events are queried from the event store that was added first using
        `add_rotated_store(...)`.

        Parameters:
        from_   -- if not None, return only events added after the event with
                   if `from_`. If None, return from the start of history.
        to      -- if not None, return only events added before, and
                   including, the event with event id `to`. If None, return up
                   to, and including, the last added event.

        returns -- an iterable of (event id, eventdata) tuples.

        """
        return self.stores[0].get_events(from_, to)
