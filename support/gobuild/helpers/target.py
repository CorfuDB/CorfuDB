# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential
"""
Contains helper classes for writing your target.  See the class
comment for more information.
"""
import helpers


class TargetException(Exception):
   """
   Simple exception to throw when things go wrong.
   """
   pass


class Target:
   """
   Target Class

   The Target class encapsulates all the information required to
   build something.  Gobuild uses it to discover everythign it
   needs to know to build your product, including what code to
   sync, what commands to run to do the build, what bits should
   be copied to storage, etc.

   The steps to add a new target to gobuild go something like:

   1) Integ the gobuild target SDK into your product branch
      in the support/gobuild directory.

   2) Create a new python file called <your product>.py in the
      support/gobuild/targets directory.

   3) In <your product>.py, create a <Your product> class that
      overrides the methods in Target (or CompoundTarget)
      with the information specific to your product.

   4) Add your product to the TARGETS list in support/gobuild/__init__.py

   5) Profit!

   For more information, consult the gobuild sdk documentation.
   """

   # This describe the version of the target interface to the
   # global gobuild scripts.  Do not touch.
   GOBUILD_INTERFACE_VERSION = "1.0"

   def GetBuildProductNames(self):
      """
      Returns the short name and a description of your product.
      The short name is used to index your product in the
      build database.  It should ideally be identical to what
      you decide to call your product in the TARGETS table in
      __init__.py (e.g. wgs, ws, vpx, server, p2v, etc).

      The long name should be the human readible name of your
      product.  It is also stored in the build database.

      Sample Implementation for product 'foobar':

         return [ { 'name'     : 'foobar',
                    'longname' : 'VMware Foobar for x86' } ]

      """
      raise TargetException("Product owner failed to implement "
                            "GetBuildProductNames")

   def GetClusterRequirements(self):
      """
      Returns a list containing the types of hosts required for
      this build.  You really only have 3 choices here:

      If you are a Windows only build, return:
         [ 'windows' ]

      If you are a Linux only build, return:
         [ 'linux' ]

      If you are a multi-host build (Windows and Linux), return:
         [ 'windows', 'linux' ]

      For multihost builds, be sure to use the 'hosttype' parameter
      to other functions to distinguish which host your Target
      class is currently running on (e.g. when you need to sync
      different code on linux than on windows).
      """
      raise TargetException("Product owner failed to implement "
                            "GetClusterRequirements")

   def GetRepositories(self, hosttype):
      """
      Return a list of dictionaries containing the SCM repositories
      to pull code from.  For now we only support perforce.  See
      the PerforceRepo function later in this file for more details.
      Build client root directory can be referenced through %(buildroot).

      Sample Implementation for product 'foobar':

         return [ PerforceRepo('foobar/%(branch)', 'foobar'),
                  PerforceRepo('foobar-tools/%(branch)', 'foobar-tools') ]
      """
      raise TargetException("Product owner failed to implement "
                            "GetRepositories")

   def GetCommands(self, hosttype):
      """
      Return a list of dictionaries describing which commands to run
      in order to build the product.  In general you should keep this
      list as *small* and *rigid* as possible.  Best practices
      are that you should have 1 item in the list which is a call
      to some dispatching languge (e.g. make, scons, bash, etc)
      which will handle building everything you need for your
      scenario.  The idea is that this file sits on the interface
      between gobuild and your code and that there's value in having
      it change as seldomly as possible.  So if you need to tweak
      your product to build something differently, we'd rather you
      tweak a Makefile than the gobuild target file.

      Each dictionary should contain:

         'desc'    - Returns a short, human readible description of what
                     the task is doing.
         'root'    - The directory relative to the %(buildroot), from where
                     the command should be run.
         'log'     - The name of a logfile for the command.  Best practices
                     are for this to end in '.log'
         'command' - The command to run, including all command line
                     arguments.
         'env'     - (optional) A dictionary containing the environement
                     to use when running this command.  If you do not require
                     any particular environment, do not specify this key/
                     value pair.  It's recommended that you use
                     absolute paths (on linux) or %TCROOT% (on windows)
                     to point to tools in the toolchain rather than
                     modifying your environment to point to them, but
                     sometimes that is not an option.

      If you're using Make or Scons, there are helper functions
      to help you make your command (see below).

      Sample Implementation for product 'foobar':

         return [ { 'desc'    : 'Compiling foobar',
                    'root'    : 'foobar',
                    'log'     : 'foobar.log',
                    'command' : self._MakeCommand(hosttype,
                                                  'foobar',
                                                  PRODUCT=foobar)
                  } ]
      """
      raise TargetException("Product owner failed to implement "
                            "GetCommands")


   def GetStorageInfo(self, hosttype):
      """
      Return a list of dictionarys containing the directories
      which need to be copied to storage after the build is done.
      Only these directories will be preserved: the rest will
      be deleted!

      Each dictionary must contain:

          'type' : The type of directory at 'src'.  This must
                   be either 'source' or 'build'.  Source
                   directories are copied verbatim.  Build
                   directories are filtered by gobuild such that
                   intermediate information (e.g. .o's and .objs)
                   are removed before copying the directory to
                   storage.

          'src'  : The path relative to the %(buildroot) of what
                   you want copied.

          'keep' : An optional list of keep specifications that specify
                   files that you always want to store onto storage.

                   In order to save space when storing builds, gobuild
                   will normally filter out intermediate binary files
                   such as .o or .d when putting files to storage. However
                   sometimes this behavior isn't wanted. A keep spec
                   allows one to define a 'whitelist' of file(s) that will
                   always be stored.

                   A keep specification can be defined to keep a single
                   file, everything under a certain directory, or all
                   files with a certain extension in a directory.

                   'dir/subdir/myimportantfile.o'
                      -- stores dir/subdir/myimportantfile.o

                   'dir/subdir/*'
                      -- recursively stores everything under dir/subdir

                   'dir/subdir/*.o'
                      -- recursively stores all files ending with .o
                         under dir/subdir

      Here are some best practices to follow:

      1) Isolate your built bits into a single directory called
         'build'.  Do not generate build output side by side with
         your source!

      2) Be sure to isolate tools that you need to check out from
         your source and build directories and do *not* copy them
         to storage!  They don't need to be preserved.  Even better,
         use only tools that have been checked into the Build
         Toolchain.

      And some just for for multihost builds:

      3) Make sure that only one host copies the source to save time.

      4) When storing your build directory, add the hosttype to
         the beginning of the path to make sure windows and linux
         buildd don't collide (e.g. 'build' -> '%s/build' % hosttype )

      If you are using make or scons there are helper methods below.

      Sample Implementation for product 'foobar':

         return [ self._GetMakeStorageRule(hosttype, 'foobar') ]

      of:

         return [ { 'type' : 'source',
                    'src'  : 'foobar'
                  }
                  { 'type' : 'build',
                    'src'  : 'foobar/build'
                  }
                ]

      """
      raise TargetException("Product owner failed to implement "
                            "GetStorageInfo")

   def GetBuildProductVersion(self, hosttype):
      """
      Returns a string in "x.y.z" format containing the version of
      the product.  This value will be stored in the build database
      to help track which builds correspond to which versions of
      your product.

      GetBuildProductVersion is guaranteed to be called after you
      build is finished while the build output is still in your
      build directory.  It is designed to make you look up the
      version from some source you computed during the build (e.g.
      in a shared header).
      """
      return ''

   def GetOptions(self):
      """
      You may override this method if you want to specify any
      additional command line options for your build.  This is
      really advanced and not recommended, but is occasionally
      required.

      GetOptions returns a list of 3-tuples containing the
      name of the flag, its default value, and a docstring 
      describing what the flag does.  Ideally you should
      put your flags in the '<product name>.' namespace to
      make sure it's globally unique with all other flags.

      Sample Implementation for product 'foobar':

         return [ ( 'foobar.enable.a', False, 'Enable feature A'),
                  ( 'foobar.enable.b', False, 'Enable feature B') ]

      """
      return []

   def SetOptions(self, options):
      """
      Gobuild will call SetOptions to notify your target of the
      options used for this build (e.g. branch, buildtype,
      reltype, etc.).  The default implementation creates a
      self.log variable that you can use for logging and stuffs
      all the options into a self.options dictionary if you need
      to look them up later.

      Unless you're doing something ultra fancy, you should not
      need to override this function.
      """
      self.log     = options['gobuild-log']
      self.options = options

   def GetComponentPath(self):
      """
      (If you are not a component, you may safely ignore this
       function)

      Returns the path relative to the %(buildroot) where your
      component will drop its deliveries.  This directory will
      be provided verbatim to everyone who depends on you as
      a component.

      If you are not a component, do not override this method!
      If you depend on other components, see GetComponentDependencies

      Sample Implementation for product 'foobar':

         return '%(buildroot)/foobar/build/%(buildtype)/stuff'

      """
      return ''

   def GetComponentDependencies(self):
      """
      (If you do not depend on any components, you may safely
       ignore this method).

      Returns a dictionary whose keys are the names of components you depend
      on, and whose values describe information about the component. If you
      consume a component just once, the value is a dictionary. If you consume
      a component more than once (from different branches), the value is a list
      of dictionaries. Each dictionary contains:

         'alias'     : An alias name for the component. Only used for
                       components consumed more than once.
         'branch'    : The name of the branch where the component
                       you depend on lives.
         'changeset' : The exact changeset of the component you'd like
                       to use.
         'buildtype' : The buildtype of the component you'd like to use.
                       If omitted, defaults to the same buildtype as
                       the consuming build.
         'hosttypes' : A dictionary mapping consumer build hosttypes to
                       producer build hosttypes. If omitted, defaults
                       to an identify mapping of the producer's
                       hosttypes.

      If you do not depend on other components, do not override this
      method!
      If are a component, see GetComponentPath.

      Sample Implementation for product 'foobar':
         comps = {}
         comps['foo'] = {
            'branch': 'foo-main',
            'changeset': 123
         }
         comps['bar'] = {
            'branch': 'bar-main',
            'changeset': 456,
            'buildtype': 'beta'
         }
         comps['baz'] = {
            'branch': 'baz-main',
            'changeset': 789,
            'hosttypes': {
               'linux-rhel5u2': 'linux',
               'windows': 'windows'
            }
         }
         return comps
      """
      return {}

   def GetComponentDependencyAliases(self):
      """
      Returns the list of component dependencies for your target.

      Don't override this method.

      Calls GetComponentDependencies() to get the dependency information, then
      returns the list of component aliases. A component alias is the same as
      the component name unless the component is being consumed more than once
      (from different branches).
      """
      aliases = []
      if hasattr(self, 'GetComponentDependencies'):
         for name, depinfolist in self.GetComponentDependencies().iteritems():
            if isinstance(depinfolist, dict):
               # Component consumed just once, so treat name as the alias.
               aliases.append(name)
            else:
               # Component consumed more than once. Use alias if one defined.
               for depinfo in depinfolist:
                  aliases.append(depinfo.get('alias', name))
      return aliases

class CompoundTarget(Target):
   """
   CompoundTarget Class

   Bundles many targets together into a single target.  Used in
   complex build scenarios where many steps used from other builds
   are required.  For example, both Workstation and Server require
   building the tools in the exact same way.  This is done by
   making a Tools target (derived from Target) and having the
   Workstation and Server targets include it by deriving from
   CompoundTarget.

   CompountTarget's need simply define a GetSubtargets method which
   returns a list of Target classes which they want to build.  The
   CompoundTarget class will handle the rest, making sure each gets
   build in the order of the sub-targets list.

   For documentation on what each method is for, see their
   definitions in the Target class, above.

   Sample:
       class BuildMeFirst(Target):
         ...

       class MainTarget(CompoundTarget):
         ...
         def GetSubtargets(self):
            return [ BuildMeFirst(), BuildMeSecond() ]
   """

   GOBUILD_INTERFACE_VERSION = "1.0"

   def GetSubtargets(self):
      """
      Override this method to provide a list of target to
      accumulate.
      """
      raise TargetException("Class derived from CompoundTarget "
                            "failed to implement GetSubtargets")

   def GetClusterRequirements(self):
      return list(set(self._Accumulate('GetClusterRequirements')))

   def SetOptions(self, options):
      Target.SetOptions(self, options)

      # Reset subtargets to None in case the target has decided
      # to change its list of subtargets are a result of getting
      # new options.
      if hasattr(self, 'subtargets'):
         delattr(self, 'subtargets')

      self._FanOut('SetOptions', options)

   def GetOptions(self):
      return self._Accumulate('GetOptions')

   def GetRepositories(self, hosttype):
      return self._Accumulate('GetRepositories', hosttype)

   def GetCommands(self, hosttype):
      return self._Accumulate('GetCommands', hosttype)

   def GetStorageInfo(self, hosttype):
      return self._Accumulate('GetStorageInfo', hosttype)

   def GetComponentDependencies(self):
      return self._AccumulateDict('GetComponentDependencies')

   def _FanOut(self, fn, *args, **options):
      for t in self._GetSubtargets():
         if hasattr(t, fn):
            getattr(t, fn)(*args, **options)

   def _Accumulate(self, fn, *args, **options):
      result = list()
      for t in self._GetSubtargets():
         if hasattr(t, fn):
            result += getattr(t, fn)(*args, **options)

      return result

   def _AccumulateDict(self, fn, *args, **options):
      result = {}
      for t in self._GetSubtargets():
         if hasattr(t, fn):
            merge = getattr(t, fn)(*args, **options)
            for m in merge:
               if m in result and result[m] != merge[m]:
                  raise TargetException(
                     "Could not resolve merge conflict in CompoundTarget "
                     "(%s: %s != %s)" % (m, result[m], merge[m]))
            result.update(merge)
      return result

   def _GetSubtargets(self):
      if not hasattr(self, 'subtargets'):
         self.subtargets = self.GetSubtargets()

      return self.subtargets


def PerforceRepo(src, dst):
   """
   The PerforceRepo function returns a dictionary describing how
   to pull code from a perforce repository.

   The 'src' parameter should be the depot path to the bits you
   need to sync.  You should use the %(branch) variable to
   specify which branch you want rather than hard coding a
   particular branch.

   The 'dst' parameter should be the relatiev directory under
   the clientroot where you'd like to store your bits.

   Sample Usage (sync both the source and tools directories
   for project foobar):

     [ PerforceRepo('foobar/%(branch)', 'foobar'),
       PerforceRepo('foobar-tools/%(branch)', 'foobar-tools'),
       OverlayPerforceRepo('foobar-overlay/%(branch)', 'foobar-overlay') ]

   """
   if src.startswith('//'):
      raise TargetException(
         'Source repository cannot be an absolute depot path')
   return {
      'rcs' : 'perforce',
      'src' : '%s%s' % (helpers.GetPrefix(), src),
      'dst' : dst,
   }

def OverlayPerforceRepo(src, dst):
   """
   This function adds support for perforce depot path, which begins
   with '+' for project repository path mapping.
   """
   q = PerforceRepo(src, dst)
   q['src'] = '+' + q['src']
   return q

