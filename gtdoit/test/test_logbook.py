import sys
import unittest
import gtdoit.logbook


class TestArgumentParsing(unittest.TestCase):
    def testAtLeastOneEndpointRequired(self):
        exitcode = gtdoit.logbook.main([], exit=False)
        self.assertEqual(exitcode, 2)

    def testOnlyStreamingEndpointFails(self):
        exitcode = gtdoit.logbook.main(['--streaming-bind-endpoint',
                                        'tcp://hello'], exit=False)
        self.assertEqual(exitcode, 2)

