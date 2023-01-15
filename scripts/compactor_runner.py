#!/usr/bin/env python3

import getpass
import sys
from subprocess import check_call

if __name__ == "__main__":
    sys.argv.pop(0)
    user = getpass.getuser()
    if user == 'root':
        check_call("sudo -u corfu python3 /usr/share/corfu/scripts/compactor_runner_as_corfu.py " + " ".join(sys.argv), shell=True)
    else:
        check_call("python3 /usr/share/corfu/scripts/compactor_runner_as_corfu.py " + " ".join(sys.argv), shell=True)
