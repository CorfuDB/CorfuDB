# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helpers for maven-based targets.
"""

import os

import helpers.target

class MavenHelper:
   """
   Helper class for targets that build with maven.
   """
   def _Command(self, hosttype, targets, mavenversion='2.2.0', mavenoptions={}, **systemproperties):
      """
      Return a dictionary representing a command to invoke maven with
      standard mavenflags.
      """

      def q(s):
         return '"%s"' % s

      defaults = {
         'GOBUILD_OFFICIAL_BUILD' :       '1',
         'GOBUILD_AUTO_COMPONENTS':       'false',
         'OBJDIR'                 :       q('%(buildtype)'),
         'RELTYPE'                :       q('%(releasetype)'),
         'BUILD_NUMBER'           :       q('%(buildnumber)'),
         'PRODUCT_BUILD_NUMBER'   :       q('%(productbuildnumber)'),
         'CHANGE_NUMBER'          :       q('%(changenumber)'),
         'BRANCH_NAME'            :       q('%(branch)'),
         'PUBLISH_DIR'            :       q('%(buildroot)/publish'),
         'BUILDLOG_DIR'           :       q('%(buildroot)/logs'),
         'REMOTE_COPY_SCRIPT'     :       q('%(gobuildc) %(buildid)'),
      }

      # Add a GOBUILD_*_ROOT flag for every component we depend on.
      for d in self.GetComponentDependencyAliases():
         d = d.replace('-', '_')
         defaults['GOBUILD_%s_ROOT' % d.upper()] = \
                           '%%(gobuild_component_%s_root)' % d

      # Override the defaults above with the systemproperties passed in by
      # the client.
      defaults.update(systemproperties)

      # Choose maven
      if hosttype.startswith('windows'):
         tcroot = os.environ.get('TCROOT', 'C:/TCROOT-not-set')
         mavencmd = '%s/noarch/apache-maven-%s/bin/mvn.bat' % (tcroot, mavenversion)
      else:
         mavencmd = '/build/toolchain/noarch/apache-maven-%s/bin/mvn' % mavenversion

      # Create the command line to invoke maven
      options = ''
      for k in sorted(defaults.keys()):
         v = defaults[k]
         options += ' -D' + str(k)
         if v is not None:
            options += '=' + str(v)

      for k in sorted(mavenoptions.keys()):
         v = mavenoptions[k]
         options += ' ' + str(k)
         if v is not None:
            options += '=' + str(v)

      cmd = '%s %s' % (mavencmd, options)

      if targets:
         target = targets
         if isinstance(targets, (list, tuple)):
             target = ' '.join(targets)

         cmd += ' ' + str(target)

      return cmd
