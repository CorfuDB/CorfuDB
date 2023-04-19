#!/usr/bin/env python3

"""
USAGE:
1. To trigger and validate compactor runs using the default parameters. If there's an ongoing compactor cycle, wait for
it to get completed and trigger the new compactor cycle and validate the triggered cycle.
/usr/share/corfu/scripts/trigger_and_validate_compactor.py --testDefault true
2. Validates a compactor run . If there's an ongoing compactor cycle, do not trigger a new cycle. Instead, wait for that
 cycle to complete and validate the same.
/usr/share/corfu/scripts/trigger_and_validate_compactor.py --port 9000 --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml
"""

from argparse import ArgumentParser
import sys

sys.path.insert(1, '/usr/share/corfu/scripts')
import compactor_utils

CORFU_YML_FILE_PATH = "/usr/share/corfu/conf/corfu-compactor-config.yml"
CORFU_PORT = "9000"

class ValidateCompactor(object):
    def __init__(self, args):
        """
        Initialize components and read user provided configuration.
        """
        self.args = args
    def run(self):
        if self.args.testDefault is True:
            corfu_validated = compactor_utils.trigger_always_and_validate(self.args, CORFU_PORT, CORFU_YML_FILE_PATH)

            status = 'COMPLETED' if corfu_validated else 'FAILED'
            print('Compaction status for corfu at port: 9000 is ' + status)

            if not corfu_validated:
                print('Compaction Failed. Compactor-Status: ' + status)
                sys.exit(1)
        else:
            compactor_utils.trigger_if_required_and_validate(self.args)

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--ifname", type=str,
                            help="The network interface that corfu server is listening to. "
                                 "Default value is eth0.",
                            required=False, default="eth0")
    arg_parser.add_argument("--port", type=str,
                            help="The corfu server port number. "
                                 "Default value is 9000.",
                            required=False, default="9000")
    arg_parser.add_argument("--compactorConfig", type=str,
                            help="The file containing config for compactor",
                            default="/usr/share/corfu/conf/corfu-compactor-config.yml",
                            required=False)
    arg_parser.add_argument("--trimAfterCheckpoint", type=bool,
                            help="To enable trim again after checkpointing all tables",
                            default=False, required=False)
    arg_parser.add_argument("--testDefault", type=bool,
                            help="Verify compactor run",
                            default=False, required=False)

    args = arg_parser.parse_args()
    validate_compactor = ValidateCompactor(args)
    validate_compactor.run()
