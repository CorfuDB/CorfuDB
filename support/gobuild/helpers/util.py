# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential
"""
Contains miscellaneous helper functions.
"""
import os
import re
import sys

from helpers.target import TargetException

def ExtractMacro(filename, macro):
   """
   Return the string value of the macro `macro' defined in `filename'.
   """
   # Simple regex is far from a complete C preprocessor but is useful
   # in many cases
   regexp = re.compile(r'^\s*#\s*define\s+%s\s+"(.+[.].+[.].+)"\s*$' % macro)
   try:
      for line in open(filename):
         m = regexp.match(line)
         if m:
            return m.group(1)
   except EnvironmentError:
      pass
   return ''


def MkdirCommand(hosttype, path, root='', logfile=None):
   """
   Return a dict that describes a command to create a directory.
   """
   if not logfile:
      logfile = 'mkdir-%s.log' % os.path.basename(path)

   if hosttype.startswith('linux'):
      mkdir = '/build/toolchain/lin32/coreutils-5.97/bin/mkdir'
   elif hosttype.startswith('macosx'):
      mkdir = '/build/toolchain/mac32/coreutils-5.97/bin/mkdir'
   elif hosttype.startswith('windows'):
      tcroot = os.environ.get('TCROOT', 'C:/TCROOT-not-set')
      mkdir = '%s/win32/coreutils-5.3.0/bin/mkdir.exe' % tcroot
   else:
      raise TargetException("Unknown hosttype %s" % hosttype)

   return {
      'desc': 'Creating directory %s' % path,
      'root': root,
      'log': logfile,
      'command': '%s -p %s' % (mkdir, path),
      'env': {},
   }


###############################################################################
#
# XXX: Borrow some util functions from scons (#scons/lib/vmware/utils.py)
#
###############################################################################

##### API for querying the build host #####

def BuildHost():
   """Return a string that identifies the host we're running on.

   Returns either 'Linux' or 'Win32' or 'Mac'. Uses sys.platform to figure it
   out.  """
   if sys.platform == 'linux2':
      return 'Linux'
   elif sys.platform == 'darwin':
      return 'Mac'
   elif sys.platform == 'win32':
      return 'Win32'

   raise ConfigError("Platform %s not supported" % (os.name))


def BuildHostIsLinux():
   """Returns True if scons is running on a Linux host
   """
   return BuildHost() == 'Linux'

def BuildHostIsWindows():
   """Returns True if scons is running on a Windows host
   """
   return BuildHost() == 'Win32'

def BuildHostIsMac():
   """Returns True if scons is running on a Mac OS host
   """
   return BuildHost() == 'Mac'


# cache the result so we don't waste resources, this function might get called
# multiple times, but it should always return the same number
_localProcessors = None

def CountLocalProcessors():
   global _localProcessors
   if _localProcessors is not None:
      return _localProcessors

   # A safe default...
   _localProcessors = 1

   if BuildHostIsWindows():
      try:
         _localProcessors = int(os.environ['NUMBER_OF_PROCESSORS'])
      except:
         pass
   try:
      import multiprocessing
      _localProcessors = multiprocessing.cpu_count()
   except:
      if BuildHostIsWindows():
         try:
            import win32api
            _localProcessors = win32api.GetSystemInfo[5]
         except:
            pass
      elif BuildHostIsLinux():
         info = os.popen('/build/toolchain/lin32/coreutils-5.97/bin/uname -a').read()
         if re.search(r'SMP', info):
            _localProcessors = int(os.popen("/build/toolchain/lin32/grep-2.5.1a/bin/grep "
                                            "-c ^bogomips /proc/cpuinfo").read())
      elif BuildHostIsMac():
         path = '/usr/sbin/sysctl'
         sysctl = path if os.path.exists(path) else 'sysctl'
         _localProcessors = int(os.popen('%s -n hw.ncpu' % sysctl).read())
   return _localProcessors

