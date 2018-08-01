# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helpers for legacy make-based targets.
"""

import helpers.make
import helpers.util

class LegacyMakeHelper(helpers.make.MakeHelper):
   """
   Helper class for targets that build with make and rely on legacy
   makefile flags such as `CPANDCS`.

   Gobuild provides compatible definitions for these flags so that
   Gobuild support can easily be added without removing support for
   p4-build.pl. This class provides a private method _Command() that
   helps to create a `make` command with the correct legacy flags, as
   well as the standard official flags.
   Please see helpers/target.py for detail usages on GetCommands
   and GetStorageInfo methods.

      >>> import helpers.env
      >>> import helpers.target
      >>> import helpers.legacymake
      >>>
      >>> class HelloWorld(helpers.target.Target, helpers.legacymake.LegacyMakeHelper):
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
      >>>

   """
   def _Command(self, hosttype, target, **flags):
      """
      Return a dictionary representing a command to invoke `make` on
      `target`, using target-specific `flags` as well as standard
      official build flags and legacy flags.
      """
      def q(s):
         return '"%s"' % s

      gotransfer = '%%(gobuildc) %%(buildid) %s %%(buildroot)/legacy/transfer '

      # Legacy flags
      defaults = {
         'RELEASE_PACKAGES'      :            q('%(buildroot)/publish'),
         'RELEASE_BINARIES'      :            q('%(buildroot)/publish'),
         'OB_RELEASE_BINARIES'   :            q('%(buildroot)/publish'),
         'CPANDCS'               :            q(gotransfer % 'cpandcs'),
         'CPANDWAIT'             :            q(gotransfer % 'cpandwait'),
         'CONFIRM'               :            q(gotransfer % 'confirm'),
         'WAIT'                  :            q(gotransfer % 'wait'),
         'GOBUILDC'              :            q('%(gobuildc) %(buildid)'),
         'CREATE_LOCK'           :            q('%(gobuildc) %(buildid) createlock'),
         'REMOVE_LOCK'           :            q('%(gobuildc) %(buildid) removelock'),
         'TRANSFER_DIRECTORY'    :            q('%(buildroot)/legacy/transfer'),
         'STATE_DIRECTORY'       :            q('%(buildroot)/legacy/state'),
         'BUILDLOG_DIR'          :            q('%(buildroot)/logs'),
      }
      defaults.update(flags)

      return helpers.make.MakeHelper._Command(self, hosttype, target, **defaults)

   def _GetVMVersion(self, product, filename='bora/public/vm_version.h'):
      """
      Return the version specified for a particular product in the
      bora public headers. `product` is a string like 'GSX'.
      """
      return helpers.util.ExtractMacro(
         '%s/%s' % (self.options.get('buildroot'), filename),
         '%s_VERSION' % product.upper())
