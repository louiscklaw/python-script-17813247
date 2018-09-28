#!/usr/bin/env python

from fabric.api import *
from fabric.colors import *
from fabric.context_managers import *
from fabric.contrib.project import *

AUTOPEP8_CMD = 'autopep8 --in-place --aggressive --aggressive'
PYLINT_CMD = 'pylint --disable=R,C'


WORKING_FILES = [
    'workable_log_parse.py'
]

@task
def tidy():
    for working_file in WORKING_FILES:
        local('%s %s' % (AUTOPEP8_CMD, working_file))

@task
def pylint():
    for working_file in WORKING_FILES:
        local('%s %s ' % (PYLINT_CMD, working_file))

@task
def develop():
    tidy()
    pylint()
