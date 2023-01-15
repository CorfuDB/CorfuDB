#!/usr/bin/env python3

import sys
from subprocess import check_call, check_output

if __name__ == "__main__":
    print(sys.argv)
    sys.argv.pop(0)
    check_output("sudo -u corfu /usr/share/corfu/scripts/compactor_runner_as_corfu.py" + " ".join(sys.argv), shell=True).decode()