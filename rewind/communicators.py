"""Classes used to simplify communication between different components."""
import zmq


class EventQuerier(object):

    """Client that queries events from rewind over ZeroMQ."""

    class QueryException(Exception):
        """Raised when rewind server returns an error.

        Usually this exception means you have used a non-existing query key.

        """
        pass

    def __init__(self, socket):
        """Constructor."""
        self.socket = socket

    def query(self, from_=None, to=None):
        """Make a query of events."""
        assert from_ is None or isinstance(from_, str)
        assert to is None or isinstance(to, str)
        first_msg = True
        done = False
        while not done:
            # _real_query(...) are giving us events in small batches
            done, events = self._real_query(from_, to)
            for eventid, eventdata in events:
                if first_msg:
                    assert eventid != from_, "First message ID wrong"
                    first_msg = False
                from_ = eventid
                yield (eventid, eventdata)

    def _real_query(self, from_, to):
        """Make the actual query for events.

        Since the Rewind streams events in batches, this method might not
        receive all requested events.

        Returns the tuple `(done, events)` where
         * `done` is a boolean whether the limited query result reached the
           end, or whether there's more events that need to be collected.
         * `events` is a list of `(eventid, eventdata)` event tuples where
          * `eventid` is a unique string the signifies the event; and
          * `eventdata` is a byte string containing the serialized event.

        """
        assert from_ is None or isinstance(from_, str), type(from_)
        assert to is None or isinstance(to, str), type(to)
        self.socket.send(b'QUERY', zmq.SNDMORE)
        self.socket.send(from_.encode() if from_ else b'', zmq.SNDMORE)
        self.socket.send(to.encode() if to else b'')

        more = True
        done = False
        events = []
        while more:
            data = self.socket.recv()
            if data == b"END":
                assert not self.socket.getsockopt(zmq.RCVMORE)
                done = True
            elif data.startswith(b"ERROR"):
                assert not self.socket.getsockopt(zmq.RCVMORE)
                raise self.QueryException("Could not query: {0}".format(data))
            else:
                if not isinstance(data, str):
                    assert isinstance(data, bytes)
                    eventid = data.decode()
                else:
                    # Python 2
                    eventid = data
                assert self.socket.getsockopt(zmq.RCVMORE)
                eventdata = self.socket.recv()

                assert isinstance(eventid, str), type(eventid)
                eventtuple = (eventid, eventdata)
                events.append(eventtuple)

            if not self.socket.getsockopt(zmq.RCVMORE):
                more = False

        return done, events
