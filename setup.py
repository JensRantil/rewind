from distutils.core import setup

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
    license='Reshipment must be approved by Jens Rantil', # TODO: Clarify
    long_description=open('README.txt').read(),
    install_requires=[
        "protobuf",
    ],
    entry_points={
        'console_scripts': [
            'logbook = gtdoit.logbook:main',
        ]
    },
)

