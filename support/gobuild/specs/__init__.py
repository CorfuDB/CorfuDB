# Copyright 2016 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Gobuild component specs.

Specfiles can be imported by gobuild targets. Consider the case where
a product 'foo' depends on a component 'bar'. The recommended way to
organize this is:

   ===== targets/foo.py =====

   import specs.foo

   class Foo:
      [...]
      def GetComponentDependencies(self):
         comps = {}
         comps['bar'] = {'branch': specs.foo.BAR_BRANCH,
                         'change': specs.foo.BAR_CLN}
         return comps

   ===== specs/foo.py =====

   BAR_BRANCH = 'main'
   BAR_CLN = 1234

This separates the spec information from the rest of the
target module and makes it easier to update on its own
(manually or automatically).
"""
