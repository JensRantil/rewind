import sys
import unittest
import gtdoit.logbook


class TestArgumentParsing(unittest.TestCase):
    """Tests command line arguments to `logbook`.

    TODO: Test the 'PROG --help' call gives expected output. Don't know how to
          override sys.exit in best way.
    """
    def testAtLeastOneEndpointRequired(self):
        exitcode = gtdoit.logbook.main([], exit=False)
        self.assertEqual(exitcode, 2)

    def testOnlyStreamingEndpointFails(self):
        exitcode = gtdoit.logbook.main(['--streaming-bind-endpoint',
                                        'tcp://hello'], exit=False)
        self.assertEqual(exitcode, 2)

