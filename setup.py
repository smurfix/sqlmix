#!/usr/bin/env python

"""Setup script for smurf's Python database module"""

from distutils.core import setup

description = "Simple syntax for any database"
long_description = \
"""
This module contains database access methods which are very easy
to use and work with any Python database module without touching
parameterized SQL syntax.

"""

setup (name = "sqlmix",
       version = "0.10.5",
       description = description,
       long_description = long_description,
       author = "Matthias Urlichs",
       author_email = "smurf@smurf.noris.de",
        classifiers=[
            'Intended Audience :: Developers',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Topic :: Database :: Front-Ends',
            'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        ],

       url = "http://smurf.noris.de/code/",
       license = 'Python',
       platforms = ['POSIX'],
       packages = ['sqlmix'],
      )
