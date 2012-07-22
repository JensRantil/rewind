"""Test code format and coding standards."""
import importlib
import inspect
import pkgutil
import unittest


def setUpModule():
    global modules
    modules = [name for _, name, ispkg in pkgutil.walk_packages(['rewind'],
                                                                'rewind.')
               if not ispkg and not name.startswith('rewind.test.') and 
                  not name.startswith('rewind.messages.')]
    assert modules, "Expected to have found a couple of modules. Did not."
    modules = map(importlib.import_module, modules)


def tearDownModule():
    """Clearing up global namespace in test_code."""
    global modules
    del modules


def _get_public_classes_from_object(obj, prepend_name=''):
    classes = [(prepend_name+name, value)
               for name, value in inspect.getmembers(obj)
               if inspect.isclass(value) and not name.startswith('_')]
    result = list(classes)
    for name, value in classes:
        partialres = _get_public_classes_from_object(value,
                                                     '{0}.'.format(name))
        result.extend(partialres)
    return result


def _get_public_classes():
    classes = []
    for module in modules:
        assert inspect.ismodule(module)
        someclasses = _get_public_classes_from_object(module,
                                                      '{0}.'.format(module.__name__))
        classes.extend(someclasses)
    return classes


def _get_public_bound_methods():
    allmethods = []
    for classname, clazz in _get_public_classes():
        assert inspect.isclass(clazz)
        methods = [(classname+'.'+name, value)
                   for name, value in inspect.getmembers(clazz)
                   if not name.startswith('_') and inspect.ismethod(value)]

        # Filtering out methods that were not defined in our modules (they were
        # derived, that is)
        methods = [(name, value) for name, value in methods
                   if inspect.getmodule(value) in modules]

        allmethods.extend(methods)
    return allmethods


def _get_public_functions():
    allfunctions = []
    for module in modules:
        functions = [(module.__name__+'.'+name, func) for name, func in inspect.getmembers(module)
                     if inspect.isfunction(func) and not name.startswith('_')]
        allfunctions.extend(functions)
    return allfunctions


class TestPydoc(unittest.TestCase):
    """Tests for pydoc."""

    def testAllPublicClasses(self):
        """Test that all public classes have a pydoc."""
        classes = _get_public_classes()
        self.assertNotEqual(len(classes), 0)
        for classname, clazz in classes:
            doc = inspect.getdoc(clazz)
            msg = "{0} lacks a Pydoc string.".format(classname)
            self.assertTrue(doc and len(doc) > 4, msg)

    def testAllPublicBoundMethods(self):
        methods = _get_public_bound_methods()
        self.assertNotEqual(len(methods), 0)
        for name, method in methods:
            doc = inspect.getdoc(method)
            msg = "{0} lacks a Pydoc string.".format(name)
            self.assertTrue(doc and len(doc) > 4, msg)

    def testAllPublicFunctions(self):
        functions = _get_public_functions()
        self.assertNotEqual(len(functions), 0)
        for name, function in functions:
            doc = inspect.getdoc(function)
            msg = "{0} lacks a Pydoc string.".format(name)
            self.assertTrue(doc and len(doc) > 4, msg)
