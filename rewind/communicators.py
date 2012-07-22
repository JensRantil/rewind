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

    def _real_query(self, from_=None, to=None):
        """Make the actual query for events.

        Since the logbook streams events in batches, this method might not
        receive all requested events.
        """
        self.socket.send('QUERY', zmq.SNDMORE)
        self.socket.send(from_ if from_ else '', zmq.SNDMORE)
        self.socket.send(to if to else '')

        more = True
        done = False
        events = []
        while more:
            data = self.socket.recv()
            if data == "END":
                assert not self.socket.getsockopt(zmq.RCVMORE)
                done = True
            elif data.startswith("ERROR"):
                assert not self.socket.getsockopt(zmq.RCVMORE)
                raise self.QueryException("Could not query: {0}".format(data))
            else:
                eventid = data
                assert self.socket.getsockopt(zmq.RCVMORE)
                eventdata = self.socket.recv()
    
                eventtuple = (eventid, eventdata)
                events.append(eventtuple)

            if not self.socket.getsockopt(zmq.RCVMORE):
                more = False

        return done, events

