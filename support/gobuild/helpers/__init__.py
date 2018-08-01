# Copyright 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helper module for creating gobuild product modules.
"""
import re

# This file needs to have type "ktext" in order to make the RCS
# keyword expansion work.
RCS_FILE = '$File: //depot/build/main/gobuild-starter-kit/support/gobuild/helpers/__init__.py $'[7:-2]

# This line needs to be modified appropriately when the gobuild
# starter kit is imported into a new branch.
BRANCH_REGEX = re.compile(r'^(//.+/)build/([^/]+)/gobuild-starter-kit/support/gobuild/')

DEFAULT_DEPOT = '//depot/'
DEFAULT_BRANCH = '%(branch)'
COMMON_BRANCH = 'main'


def GetBranch():
   """
   Return the name of the branch.
   """
   m = BRANCH_REGEX.match(RCS_FILE)
   if m:
      return m.group(2)
   return DEFAULT_BRANCH


def GetCommonBranch():
   """
   Return the name of the branch for unbranched files (i.e., esxrpms,
   esx2xrpms, crosscompile, opensource).
   """
   if GetPrefix().startswith(DEFAULT_DEPOT):
      return COMMON_BRANCH
   return DEFAULT_BRANCH


def GetPrefix():
   """
   Return a common prefix where files are synced from revision control.
   """
   m = BRANCH_REGEX.match(RCS_FILE)
   if m:
      return m.group(1)
   return DEFAULT_DEPOT
