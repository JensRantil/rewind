GTDoit
======

GTDoit is a task manager based on David Allen's book Getting Things Done
(abbreviated GTD).

Developing
----------

To start developing you need to install ZeroMQ on your system
beforehand.

To install:
`python ./bootstrap.py`
`./bin/buildout`

Running tests
-------------

To run tests:
`./bin/test`

To get test coverage report you will manually have to install the
`coverage` module. This can be done by issuing `pip install coverage`.
If you are not interested in bloating your global Python namespace, you
can install `coverage` in a `virtualenv`. Note that this requires that
buildout has been bootstrapped using
`VIRTENVPATH/bin/python bootstrap.py` instead. Otherwise the `coverage`
module wont be picked up.

Author
------
This package has been developed by Jens Rantil <jens.rantil@gmail.com>.
