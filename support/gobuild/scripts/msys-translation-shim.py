"""
Translate GOBUILD_X_ROOT variables to MSYS-style paths and re-execute
command line.
"""
import os
import re
import subprocess
import sys


_SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]


def main(args):
   new_args = []
   for arg in args:
      # Translate: GOBUILD_X_ROOT=C:/foo/... => GOBUILD_X_ROOT=/c/foo/...
      m = re.match('^(GOBUILD_\w+_ROOT)=(\w):/(.+)$', arg)
      if m:
         new_arg = '%s=/%s/%s' % (m.group(1), m.group(2).lower(), m.group(3))
      else:
         new_arg = arg
      new_args.append(new_arg)
   print >> sys.stderr, '%s: running: %s' % (_SCRIPT_NAME, ' '.join(new_args))
   return subprocess.call(new_args)


if __name__ == '__main__':
   sys.exit(main(sys.argv[1:]))
