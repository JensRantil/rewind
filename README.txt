======
GTDoit
======

GTDoit is a task manager based on David Allen's book Getting Things Done
(abbreviated GTD). It is based on event sourcing and uses ZeroMQ for
communication between the different modules.

Developing
==========
Getting started developing `gtdoit` is quite straightforward. The
library uses `setuptools` and standard Python project layout for tests
etcetera.

Checking out
------------
To start developing you need to install the ZeroMQ library on your system
beforehand.

This is how you check out the `gtdoit` library into a virtual environment:

    cd <your development directory>
    virtualenv --note-site-packages gtdoit
    cd gtdoit
    git clone http://<gtdoit GIT URL> src

Workin' the code
----------------
Every time you want to work on `gtdoit` you want to change directory
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
Spelling mistakes, bad grammar, test improvements and other feature
additions are all welcome. Please issue pull requests or create an
issue if you'd like to discuss it on Github.

Architecture <stub>
===================

The `logbook`
-------------
The `gtdoit` is based on the CQRS model and uses event sourcing for
storing all state changes of the system. The `logbook` server is
responsible for receiving, forwarding and storing all events. It uses
event stores for this. This means that as long as `logbook` is running
events will be stored securely in the system.

TODO: More to come!

Author
======
This package has been developed by Jens Rantil <jens.rantil@gmail.com>.
