# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

import os

"""
Contains helper methods for working with the environment.
"""

def SafeEnvironment(hosttype):
   """
   Returns a minimal enviorment dictionary suitable for ensuring build
   isolation.  Basically we want to make sure the path doesn't point
   to things which may be currently installed on the build machine.
   """
   env = os.environ.copy()
   path = []
   if hosttype.startswith('windows'):
      if env.get('SYSTEMROOT'):
         path.append(os.path.join(env['SYSTEMROOT'], 'system32'))
         path.append(env['SYSTEMROOT'])

         # make and cygwin sh are both case sensitive and python
         # automagically capitiaizes the keys in os.environ.  Put
         # then back.
         env['SystemRoot'] = env['SYSTEMROOT']
         del env['SYSTEMROOT']
         env['ComSpec'] = env['COMSPEC']
         del env['COMSPEC']

   env['PATH'] = os.pathsep.join(path)
   env['PYTHONDONTWRITEBYTECODE'] = ''
   return env
