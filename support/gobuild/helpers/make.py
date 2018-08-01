# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential
"""
Helpers for make-based targets.
"""
import os

import helpers.target


class MakeHelper:
   """
   Helper class for targets that build with make.

   Meant to be used as a mixin with helpers.target.Target. Provides a
   private method _Command() that helps to create a make command, as
   well as two private methods _GetStoreSourceRule() and
   _GetStoreBuildRule() that help to create storage rules.
   Please see helpers/target.py for detail usages on GetCommands
   and GetStorageInfo methods.

      >>> import helpers.env
      >>> import helpers.target
      >>> import helpers.make
      >>>
      >>> class HelloWorld(helpers.target.Target, helpers.make.MakeHelper):
      ...    def GetCommands(self, hosttype):
      ...       # Make target name to be executed to build project.
      ...       target = 'helloworld'
      ...       # Make flags to be passed in.
      ...       flags = {'PRODUCT': 'hello'}
      ...       # Root directory, from where make command is to be executed.
      ...       root = '%(buildroot)'
      ...       # Environment settings under where make command is to be executed.
      ...       env = helpers.env.SafeEnvironment(hosttype)
      ...       return [ { 'desc'    : 'Running hello world sample',
      ...                  'root'    : root,
      ...                  'log'     : '%s.log' % target,
      ...                  'command' : self._Command(hosttype, target, **flags),
      ...                  'env'     : env,
      ...                } ]
      ...    def GetStorageInfo(self, hosttype):
      ...       storinfo = []
      ...       if hosttype == 'linux':
      ...          storinfo += self._GetStoreSourceRule('bora')
      ...       storinfo += self._GetStoreBuildRule(hosttype, 'bora')
      ...       return storinfo
      >>>
   """
   def _Command(self, hosttype, target, makeversion='3.81', mingwversion='20111012', use_msys=False, **flags):
      """
      Return a dictionary representing a command to invoke make with
      standard makeflags.
      """
      def q(s):
         return '"%s"' % s

      defaults = {}

      # Handle officialkey
      if 'officialkey' in self.options:
         if self.options.get('officialkey'):
            self.log.debug("Build will use official key")
            defaults['OFFICIALKEY'] = '1'
         else:
            self.log.debug("Build will not use official key")
            defaults['OFFICIALKEY'] = ''

      # Copy deliverables to publish directory
      defaults['PUBLISH_DIR'] = q('%(buildroot)/publish')

      # Enable cross-host file transfer
      defaults['REMOTE_COPY_SCRIPT'] = q('%(gobuildc) %(buildid)')

      # Disable auto-components and all specify component paths explicitly
      defaults['GOBUILD_AUTO_COMPONENTS'] = ''
      for d in self.GetComponentDependencyAliases():
         d = d.replace('-', '_')
         defaults['GOBUILD_%s_ROOT' % d.upper()] = '%%(gobuild_component_%s_root)' % d

      defaults.update(flags)

      return self._BaseCommand(hosttype, target, makeversion, mingwversion, use_msys, **defaults)

   def _DevCommand(self, hosttype, target, makeversion='3.81', mingwversion='20111012', use_msys=False, **flags):
      """
      Return a dictionary representing a command to invoke make with
      standard makeflags.
      """
      defaults = {}

      # Enable auto-components (default), request official builds for
      # missing components, and wait for these builds to complete
      defaults['GOBUILD_AUTO_COMPONENTS_REQUEST'] = '1'
      defaults['GOBUILD_AUTO_COMPONENTS_WAIT'] = '1'
      defaults['SHARED_BUILD_MACHINE'] = '1'

      defaults.update(flags)

      return self._BaseCommand(hosttype, target, makeversion, mingwversion, use_msys, **defaults)

   def _BaseCommand(self, hosttype, target, makeversion, mingwversion, use_msys, **flags):
      """
      Return a dictionary representing a command to invoke make with
      standard makeflags.
      """
      def q(s):
         return '"%s"' % s

      defaults = {
         'SHARED_BUILD_MACHINE'   :       '1',
         'SIGN_RELEASE_BINARIES'  :       '1',
         'OBJDIR'                 :       q('%(buildtype)'),
         'RELTYPE'                :       q('%(releasetype)'),
         'BUILD_NUMBER'           :       q('%(buildnumber)'),
         'PRODUCT_BUILD_NUMBER'   :       q('%(productbuildnumber)'),
         'CHANGE_NUMBER'          :       q('%(changenumber)'),
         'BRANCH_NAME'            :       q('%(branch)'),
         'BUILDLOG_DIR'           :       q('%(buildroot)/logs'),
      }
      # Handle verbosity
      if self.options.get('verbose'):
         defaults['VERBOSE'] = '3'

      # If the user has overridden the number of cpus to use for this
      # build, stick that on the command line, too.
      if self.options.get('numcpus'):
         self.log.debug('Overriding num cpus (%s).' % self.options['numcpus'])
         defaults['--jobs'] = self.options['numcpus']

      # Override the defaults above with the options passed in
      defaults.update(flags)

      # Choose make
      if hosttype.startswith('linux'):
         makecmd = '/build/toolchain/lin32/make-%s/bin/make' % makeversion
      elif hosttype.startswith('windows'):
         tcroot = os.environ.get('TCROOT', 'C:/TCROOT-not-set')
         if use_msys:
            # Use Python shim to translate gobuild component paths into MSYS-style:
            # e.g., GOBUILD_X_ROOT=C:/foo/... => GOBUILD_X_ROOT=/c/foo/...
            makecmd = '%s/win32/python-2.6.1/python.exe -B support/gobuild/scripts/msys-translation-shim.py %s/win32/mingw-%s/msys/1.0/bin/make.exe' % (tcroot, tcroot, mingwversion)
         else:
            makecmd = '%s/win32/make-%s/make.exe' % (tcroot, makeversion)
      elif hosttype.startswith('mac'):
         makecmd = '/build/toolchain/mac32/make-%s/bin/make' % makeversion
      else:
         raise helpers.target.TargetException('unsupported hosttype: %s'
                                              % hosttype)

      # Create the command line to invoke make
      cmd = [makecmd, target]
      for k in sorted(defaults.keys()):
         v = defaults[k]
         if v is None:
            cmd.append('%s' % k)
         else:
            cmd.append('%s=%s' % (k, v))
      return ' '.join(cmd)

   def _GetStoreSourceRule(self, tree):
      """
      Return the standard storage rules for a make based build.  The
      Linux side is responsible for copying the source files to storage.
      """
      return [{'type': 'source', 'src': '%s/' % tree}]

   def _GetStoreBuildRule(self, hosttype, tree):
      """
      Return the standard storage rules for a make based build.  The
      Linux side is responsible for copying the source files to storage.
      """
      return [{'type': 'build', 'src': '%s/build' % tree}]
