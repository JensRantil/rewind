from setuptools import setup
import sys


py_version = sys.version_info[:2]
PY3 = py_version[0] == 3
if PY3:
    raise RuntimeError('Python 3 is currently not supported. See issue #12.')
else:
    if py_version <= (2, 6):
        raise RuntimeError("Python <= 2.6 does not ship with argparse. "
                           "Therefore, rewind will not work with these.")


setup(
    name='rewind',
    version='0.1dev',
    author='Jens Rantil',
    author_email='jens.rantil@gmail.com',
    license='GNU AGPL, version 3',
    url='https://github.com/JensRantil/rewind',
    packages=[
        'rewind',
        'rewind.test'
    ],
    long_description=open('DESCRIPTION.rst').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: Other Audience",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Object Brokering",
        "Topic :: System :: Distributed Computing",
    ],
    setup_requires=[
        'nose>=1.0',
        'coverage==3.5.1',
    ],
    install_requires=[
        "pyzmq==2.2.0",
    ],
    tests_require=[
        "mock==0.8",
    ],
    test_suite="rewind.test",
    entry_points={
        'console_scripts': [
            'rewind = rewind.logbook:main',
        ]
    },
)

