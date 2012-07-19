Have you ever been nervous of all those DBMSs schema changes when you
are deploying your applications? They are gonna take too long, or break
backward compatibility? Have you ever thought "Crap, I wish I had stored
that information since earlier"? Have you ever felt your writing
patterns and your reading patterns differ a lot, making things harder to
scale?

CQRS (Command-Query Response Segregation) is an architectural pattern
that aims to solve these issues by splitting up your architectural
system into two parts:

* A *write side* that takes care of validating input and optimizes for
  fast writes. The write side takes commands and outputs corresponding
  events if the command validates correctly.

* A *read side* that listens to incoming events from the write side. The
  read side is optimized for fast reads.

A core concept in CQRS is the *event store* which sits inbetween the
write and the read side. The event store takes care of three things:

* persisting all events to disk.
  
* being a hub/broker replicating all events from the write to the read
  side of things.
  
* it allows fast querying of events so that different parts of the system
  can be synced back on track and new components can be brought back in
  play.

``rewind`` is an event store application that talks ZeroMQ. It is written
in Python and supports multiple storage backends.
