=======
Rewind
=======

Have you ever been nervous of all those DBMSs schema changes when you
are deploying your applications? They are gonna take too long, or break
backward compatibility? Have you ever thought "Crap, I wish I had stored
that information since earlier"? Have you ever felt your writing
patterns and your reading patterns differ a lot, making things harder to
scale?

CQRS (Command-Query Response Segregation) is an architectural pattern
that has been gaining a lot of interest that aims to solve these issues. 

`rewind` is an event store application that talks ZeroMQ. It is written
in Python and supports mutliple backends.

Installing
==========
TODO: Rewind will soon end up no PyPi.

Talking to `rewind`
===================

Developing
==========
Getting started developing `rewind` is quite straightforward. The
library uses `setuptools` and standard Python project layout for tests
etcetera.

Checking out
------------
To start developing you need to install the ZeroMQ library on your system
beforehand.

This is how you check out the `rewind` library into a virtual environment:

    cd <your development directory>
    virtualenv --note-site-packages rewind
    cd rewind
    git clone http://<rewind GIT URL> src

Workin' the code
----------------
Every time you want to work on `rewind` you want to change directory
into the source folder and activate the virtual environment scope (so
that you don't touch the global Python environment):

    cd src
    source ../bin/activate

The first time you've checked the project out, you want to initialize
development mode:

    python setup.py develop

Runnin' them tests
------------------
Running the test suite is done by issuing

    python setup.py nosetests

. Nose is configured to automagically spit out test coverage information
after the whole test suite has been executed.

As always, try to run the test suite *before* starting to mess with the
code. That way you know nothing was broken beforehand.

Helping out
===========
Spelling mistakes, bad grammar, new storage backends, test improvements
and other feature additions are all welcome. Please issue pull requests
or create an issue if you'd like to discuss it on Github.

Architecture <stub>
===================

Why the name `rewind`?
=============
 * `rewind` can look at what happened in the past and replay the events
   since then.
 * it's time to rewind and rethink the way we are overusing DBMS's and
   the way we are storing our data.

Author
======
This package has been developed by Jens Rantil <jens.rantil@gmail.com>.
