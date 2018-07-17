# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential
"""
Helpers for SCons-based targets.
"""
class SConsHelper:
   """
   Helper class for targets that build with SCons.
   """
   def _Command(self, hosttype, target, scons='scons/bin/scons', **flags):
      """
      Return a string representing an official command to invoke scons
      in bora.
      """
      defaults = {}

      # Copy deliverables to publish directory
      defaults['RELEASE_PACKAGES_DIR'] = q('%(buildroot)/publish')

      # Enable cross-host file transfer
      defaults['REMOTE_COPY_SCRIPT'] = q('%(gobuildc) %(buildid)')

      # Disable auto-components and all specify component paths explicitly
      defaults['GOBUILD_AUTO_COMPONENTS'] = False
      aliases = self.GetComponentDependencyAliases()
      if aliases:
         comp_flags = ['%s=%%(gobuild_component_%s_root)' % (d.upper(), d)
                       for d in aliases]
         defaults['GOBUILD_COMPONENTS'] = ','.join(comp_flags)

      defaults.update(flags)

      return self._BaseCommand(hosttype, target, scons, defaults)

   def _DevCommand(self, hosttype, target, scons='scons/bin/scons', **flags):
      """
      Return a string representing a developer command to invoke scons
      in bora.
      """
      defaults = {}

      # Enable auto-components (default), request official builds for
      # missing components, and wait for these builds to complete
      defaults['GOBUILD_AUTO_COMPONENTS_REQUEST'] = True
      defaults['GOBUILD_AUTO_COMPONENTS_WAIT'] = True

      defaults.update(flags)

      return self._BaseCommand(hosttype, target, scons, defaults)

   def _BaseCommand(self, hosttype, target, scons, **flags):
      """
      Return a string representing a command to invoke scons in bora
      with the standard flags.
      """
      def q(s):
         return '"%s"' % s

      defaults = {
         'BUILDTYPE'              :       q('%(buildtype)'),
         'RELTYPE'                :       q('%(releasetype)'),
         'BUILD_NUMBER'           :       q('%(buildnumber)'),
         'PRODUCT_BUILD_NUMBER'   :       q('%(productbuildnumber)'),
         'CHANGE_NUMBER'          :       q('%(changenumber)'),
         'BRANCH_NAME'            :       q('%(branch)'),
         'BUILDLOG_DIR'           :       q('%(buildroot)/logs'),
      }

      # Handle verbosity
      if self.options.get('verbose'):
         defaults['VERBOSE'] = True

      defaults.update(flags)

      # Create the command line to invoke scons
      cmd = [scons, target]
      for k in sorted(defaults.keys()):
         v = defaults[k]
         if v is None:
            cmd.append('%s' % k)
         else:
            cmd.append('%s=%s' % (k, v))
      cmd.append('--debug=stacktrace')
      return ' '.join(cmd)

   def _GetStoreSourceRule(self, hosttype, tree):
      """
      Return the standard storage rules for a scons based build.  The
      Linux side is responsible for copying the source files to storage.
      """
      return [{'type': 'source', 'src': '%s/' % tree}]

   def _GetStoreBuildRule(self, hosttype, tree):
      """
      Return the standard storage rules for a scons based build.  The
      Linux side is responsible for copying the source files to storage.
      """
      return [{'type': 'build', 'src': 'build'}]
