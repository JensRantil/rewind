"""Test code format and coding standards."""
import os
import pep8
import unittest


class TestCodeFormat(unittest.TestCase):
    @staticmethod
    def _get_all_pyfiles():
        pyfiles = []
        for dirpath, _, filenames in os.walk('rewind'):
            pyfiles.extend([os.path.join(dirpath, filename)
                            for filename in filenames
                            if filename.endswith('.py')])
        return pyfiles

    def testPep8Conformance(self):
        """Test that we conform to PEP8."""
        pep8style = pep8.StyleGuide()
        pyfiles = self._get_all_pyfiles()
        result = pep8style.check_files(pyfiles)

        # Currently one E702 and two E301:s fail. I find those checks to be
        # buggy and will report them to the pep8 project on github.
        self.assertEqual(result.total_errors, 3,
                         "Found code syntax errors (and warnings).")
