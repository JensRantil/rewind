from distutils.core import setup

# TODO: Make sure that we are using Python >= 2.7. This needs to be required
# since we are using the new(er) argparse module we need to enforce this
# requirement. See supervisor's setup.py for details on how to do this.

setup(
    name='GTDoit',
    version='0.1dev',
    author='Jens Rantil',
    author_email='jens.rantil@gmail.com',
    license='LICENSE.txt',
    packages=[
        'gtdoit',
        'gtdoit.test'
    ],
    long_description=open('README.txt').read(),
    install_requires=[
        "protobuf", # TODO: Add version
        "pyzmq",    # TODO: Add version
    ],
    entry_points={
        'console_scripts': [
            'logbook = gtdoit.logbook:main',
        ]
    },
)

