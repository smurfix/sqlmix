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
       version = "0.9",
       description = description,
       long_description = long_description,
       author = "Matthias Urlichs",
       author_email = "smurf@smurf.noris.de",
       url = "http://smurf.noris.de/code/",
       license = 'Python',
       platforms = ['POSIX'],
       packages = ['sqlmix'],
      )
