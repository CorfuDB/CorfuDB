#!/usr/bin/python -u

import argparse
import ast
import buildapi
import logging
import re
import subprocess
import sys


logger = logging.getLogger("get_repo_commit")
logger.setLevel(logging.INFO)

GOBUILD = '/build/apps/bin/gobuild'


class RaiseErrorArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise Exception(message)


def update_component_commits(components, target_options={}):
    """ update component dict with changeset """

    # GetComponentDependencies() is possibly called by gobuild-target-info.py.
    # This hack grabs the command line arguments to gobuild-target-info.py
    # to find a useful commit.

    logger.debug('##########################################################')
    logger.debug(sys.argv)
    parser = RaiseErrorArgumentParser()
    parser.add_argument('--changenumber', dest='changenumber', default=None)
    parser.add_argument('--branch', dest='branch', default=None)
    parser.add_argument('--bootstrap', dest='bootstrap', default=None)
    parser.add_argument('--targetdir', dest='targetdir', default=None)
    parser.add_argument('--buildnumber', dest='buildnumber', default=1)
    parser.add_argument("target_name")
    try:
        args, unknown = parser.parse_known_args(sys.argv[1:])
    except:
        return buildapi.update_component_commits(components)

    # After some investigation, it appears that multiple scripts call
    # GetComponentDependencies(). The final gobuild script that calls
    # GetComponentDependencies() is gobuild-master.py. It is also the only
    # script that cares about the output of GetComponentDependencies().
    #
    # It is observed that gobuild-master.py will contain --buildnumber arg
    # for both sandbox and official builds.
    # This hack checks the buildnumber - 1 if it is a real official build.
    # If buildnumber -1 is real official build, then buildnumber is also an
    # official build.
    prev_buildnumber = int(args.buildnumber) - 1
    if not buildapi.check_build_exist(prev_buildnumber, build_context="ob"):
        return buildapi.update_component_commits(components)

    # use changenumber if it is passed as argument to gobuild-target-info.py
    if args.changenumber:
        commitid = args.changenumber
        logger.debug('Choosing changenumber: %s' % commitid)
    # If no changenumber, use branch to get HEAD commit.
    elif args.branch:
        target_name = args.target_name
        branch = args.branch
        if args.bootstrap:
            bootstrap = '--bootstrap=\'%s\'' % args.bootstrap
        else:
            bootstrap = ''

        # Use "gobuild target bootstrap" command to get server and repo info
        cmd = '%s target bootstrap %s %s --branch=%s --output=json' % (
              GOBUILD, target_name, bootstrap, branch)
        output = get_output_from_cmd(cmd)
        output_dict = ast.literal_eval(output)
        server = output_dict['server']
        repo_path = output_dict['root']

        # Use "gobuild target servers" command to find rcs_port (e.g. git-eng)
        cmd = '%s target servers' % (GOBUILD)
        output = get_output_from_cmd(cmd)
        rcs_port = re.search('(\S+)\s+(\S+)\s+%s' % server, output).group(1)
        rcs_path = "%s:%s/..." % (rcs_port, repo_path)

        # Use "gobuild rcs lastchange" to get latest commit from repo
        cmd = '%s rcs lastchange \'%s\'' % (GOBUILD, rcs_path)
        commitid = get_output_from_cmd(cmd).strip()
        logger.debug('Choosing changenumber from branch: %s' % commitid)
    # No changenumber or branch, just get latest good builds' changenumbers
    else:
        return buildapi.update_component_commits(components)

    for target in components:
        if not 'change' in components[target]:
            components[target]['change'] = commitid
    logger.debug('components: %s' % components)
    return components


def get_output_from_cmd(cmd):

    logger.debug("Get output from command: %s" % cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    p.wait()
    return p.stdout.read()

