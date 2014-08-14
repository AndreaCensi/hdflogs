import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

version = "1.0dev1"


setup(
    name='hdflogs',
    author="Andrea Censi",
    author_email="censi@mit.edu",
    url='http://github.com/AndreaCensi/hdflogs',
    version=version,
    description="Simple logging of timestamped numpy data in HDF",

    long_description=read('README.md'),
    keywords="",
    license="LGPL",

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: GNU Library or '
        'Lesser General Public License (LGPL)',
    ],

    package_dir={'':'src'},
    packages=find_packages('src'),
    entry_points={
     'console_scripts': [
       # 'rawlogs = rawlogs.programs:rawlogs_main'
      ]
    },
    install_requires=[
        'tables',
        'PyContracts',
        'rawlogs',
        'DecentLogs', 
    ],

    tests_require=['nose']
)

