from setuptools import setup
import sys


py_version = sys.version_info[:2]
if py_version <= (2, 6):
    raise RuntimeError("Python <= 2.6 does not ship with argparse. "
                        "Therefore, rewind will not work with these.")


setup(
    name='rewind',
    version='0.1.2',
    author='Jens Rantil',
    author_email='jens.rantil@gmail.com',
    license='GNU AGPL, version 3',
    url='https://github.com/JensRantil/rewind',
    packages=[
        'rewind',
        'rewind.test'
    ],
    description='Rewind is a (CQRS) event store that talks ZeroMQ.',
    long_description=open('DESCRIPTION.rst').read(),
    classifiers=[
        "Development Status :: 4 - Beta",
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
    keywords="CQRS, event sourcing",
    setup_requires=[
        'nose>=1.0',
        'coverage==3.5.1',
    ],
    install_requires=[
        "pyzmq==2.2.0",
    ],
    tests_require=[
        "mock==0.8",
        "pep8==1.3.3",
        "pep257==0.2.0",
    ],
    test_suite="rewind.test",
    entry_points={
        'console_scripts': [
            'rewind = rewind.rewind:main',
        ]
    },
)

