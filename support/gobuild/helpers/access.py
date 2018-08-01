# Copyright 2009 VMware, Inc.  All rights reserved. -- VMware Confidential
"""
Helpers for access targets.
"""
import os


def MakeAccessTarget(name, cls):
   class AccessTarget(cls):
      def GetBuildProductNames(self):
         return {
            'name': "%s-access" % name,
            'longname': "Check accessed files for %s target." % name,
         }

      def GetClusterRequirements(self):
         """
         Modify the hosttypes to request exclusive leases.

         This ensures that the builds will run in isolation.
         """
         hosttypes = cls.GetClusterRequirements(self)
         if isinstance(hosttypes, list):
            hosttypes = dict((x, {}) for x in hosttypes)
         for hosttype, flags in hosttypes.iteritems():
            flags['exclusive'] = True
         return hosttypes

      def GetCommands(self, hosttype):
         """
         Return the list of commands to run for this access build.

         The list of commands is the list of commands to run the normal build,
         preceeded by a command to prepare the access build state and followed
         by commands to find accessed files in each source directory and the
         toolchain.
         """
         env = os.environ.copy()
         accessdirs = self._GetAccessDirs(hosttype)
         buildcmds = cls.GetCommands(self, hosttype)
         commands = []

         if hosttype.startswith('windows'):
            check_accessed_files = os.path.join(
               '%BUILDAPPSROOT%',
               'bin',
               'check-accessed-files.cmd',
            )
            tcroot = '%TCROOT%'
         else:
            check_accessed_files = '/build/apps/bin/check-accessed-files'
            tcroot = '/build/toolchain/'

         # Create the command to prepare the access build state.
         commands.append({
            'desc': 'Run `check-accessed-files start`',
            'root': '',
            'log': 'access_start.log',
            'command': '%s start' % check_accessed_files,
            'env': env,
         })

         # Add the actual build commands.
         commands += buildcmds

         # Find accessed files in the toolchain.
         commands.append({
            'desc': 'Find accessed files in the toolchain',
            'root': '',
            'log': 'access_find_toolchain.log',
            'command': '%s finish %s' % (check_accessed_files, tcroot),
            'env': env,
         })

         # Find accessed files in each synced directory.
         for d in accessdirs:
            cmd = [check_accessed_files, 'finish', d]
            for exclude in self._GetBuildDirs(hosttype):
               cmd.append('--exclude-dir=%s' % exclude)
            commands.append({
               'desc': 'Find accessed files in %s' % d,
               'root': '',
               'log': 'access_find_%s.log' % d,
               'command': ' '.join(cmd),
               'env': env,
            })
         return commands

      def _GetAccessDirs(self, hosttype):
         """
         Yield a sequence of source directories to check for accessed files.
         """
         for repo in self.GetRepositories(hosttype):
            yield repo['dst']

      def _GetBuildDirs(self, hosttype):
         """
         Yield a sequence of build directories for this build.
         """
         if hasattr(cls, 'GetStorageInfo'):
            for storinfo in cls.GetStorageInfo(self, hosttype):
               if storinfo['type'] != 'build':
                  continue
               yield storinfo['src']

      def GetStorageInfo(self, hosttype):
         # We don't need to save build output for access build.
         return []

   return AccessTarget
