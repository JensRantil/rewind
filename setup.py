from setuptools import setup

# TODO: Make sure that we are using Python >= 2.7. This needs to be required
# since we are using the new(er) argparse module we need to enforce this
# requirement. See supervisor's setup.py for details on how to do this.

tests_require = [
    "mock==0.8",
]

setup(
    name='rewind',
    version='0.1dev',
    author='Jens Rantil',
    author_email='jens.rantil@gmail.com',
    license='GNU AGPL, version 3',
    packages=[
        'rewind',
        'rewind.test'
    ],
    long_description=open('README.txt').read(),
    setup_requires=[
        'nose>=1.0',
        'coverage==3.5.1',
    ],
    install_requires=[
        "protobuf==2.4.1",
        "pyzmq==2.2.0",
    ],
    tests_require=tests_require,
    test_suite="rewind.test",
    entry_points={
        'console_scripts': [
            'logbook = rewind.logbook:main',
        ]
    },
)

