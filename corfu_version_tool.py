#!/usr/bin/env python

"""
This script, when executed inside corfudb repository, will generate and update
project version of all pom.xml files.
"""

from __future__ import print_function
from argparse import ArgumentParser
from datetime import datetime
from random import randint
import re
from subprocess import check_call, check_output
import sys

PATTERN = r"<version>([0-9.]+)</version>"
PATTERN_TANUKI = r"/usr/share/corfu/lib/cmdlets-([0-9.]+)-shaded.jar"

def update_version_number(base_version=None):
    """
    list(int) -> None
    Update project version number for corfudb repository.

    base_version is consist of [major version, minor version, patch version].
    If base_version is not given, it is detected from current project.
    A new version number will be generated automatically and updated in each pom file.
    """
    old_version = verify_and_get_version_number()
    if base_version != None:
        assert len(base_version) == 3 and len(filter(lambda x: type(x) == int, base_version)) == 3, \
            "Base version {} doesn't fit into format [major, minor, patch]".format(base_version)
        new_version = generate_new_version_number(base_version)
    else:
        new_version = generate_new_version_number(old_version)
    write_version_string_to_pom(new_version)
    write_version_string_to_tanuki_wrapper(new_version)

def set_version_number(version):
    """
    str -> None
    Set a specified project version number for corfudb repository.
    """
    verify_and_get_version_number()
    new_version_to_check = string_to_version_number(version)
    assert len(new_version_to_check) == 5 and \
           len(filter(lambda x: type(x) == int, new_version_to_check)) == 5, \
        "The provided version {} doesn't fit into format a.b.c.d.e".format(new_version_to_check)
    write_version_string_to_pom(version)
    write_version_string_to_tanuki_wrapper(version)

def verify_and_get_version_number():
    """
    None -> list(int)
    Verify if all pom files set the same version number, then return it.

    The format of return value is a list of int that forms a valid version number.
    If verification doesn't pass, print error message and exit.
    """
    pom_files = check_output("find . -name pom.xml", shell=True).splitlines()
    version = None
    mismatch = False
    for pom_file in pom_files:
        with open(pom_file, "r") as pf:
            for line in pf:
                re_match = re.search(PATTERN, line)
                if re_match != None:
                    found_version = re_match.group(1)
                    if version == None:
                        version = found_version
                    elif version != found_version:
                        mismatch = True
                    break  # only first <version> tag should be checked
        if mismatch:
            break
    if mismatch:
        print("[ERROR] Version numbers in pom files don't match!")
        print("[ERROR] Found version numbers {} and {}.".format(version, found_version))
        sys.exit(-1)
    return map(lambda x: int(x), version.split("."))

def generate_new_version_number(old_version):
    """
    list(int) -> str
    Generate new version number based on the old version.

    The generated version number contains five parts:
      - major version
      - minor version
      - patch version
      - datetime (as int)
      - random postfix (as int)
    """
    assert len(old_version) == 3 or len(old_version) == 5, \
        "[ERROR] The parsed version number {} is not 3 or 5 parts!".format(version_number_to_string(old_version))
    timestamp = int(datetime.utcnow().strftime("%Y%m%d%H%M%S"))
    rand = randint(1, 9999)  # exclude 0 which will be trimmed by maven
    new_version = old_version[:]
    if len(new_version) == 3:
        new_version.append(timestamp)
        new_version.append(rand)
    else:
        new_version[3] = timestamp
        new_version[4] = rand
    return version_number_to_string(new_version)

def version_number_to_string(version):
    """
    list(int) -> str
    Convert the version number list to string format.
    """
    return ".".join(map(lambda x: str(x), version))

def string_to_version_number(s):
    """
    str -> list(int)
    Convert the string to version number list.
    Exit if the format of string is not correct.
    """
    s_list = s.split(".")
    try:
        v_list = [int(x) for x in s_list]
    except ValueError:
        assert False, "[ERROR] Failed to convert to numbers from version string!"
    return v_list

def write_version_string_to_pom(new_version):
    """
    str -> None
    Replace old_version with new_version in all pom files.
    """
    pom_files = check_output("find . -name pom.xml", shell=True).splitlines()
    for pom_file in pom_files:
        content = []
        with open(pom_file, "r") as pf:
            content = pf.readlines()
        with open(pom_file, "w") as pf:
            replaced = False
            i = 0
            while i < len(content):
                line = content[i]
                if not replaced:
                    result = re.sub(PATTERN, "<version>" + new_version + "</version>", line)
                    if result != line:
                        content[i] = result
                        replaced = True
                i += 1
            pf.writelines(content)

def write_version_string_to_tanuki_wrapper(new_version):
    """
    str -> None
    Replace old_version with new_version in all tanuki wrapper files.
    """
    tanuki_files = check_output("find . -name \"corfu-server*.conf\"", shell=True).splitlines()
    for tanuki_file in tanuki_files:
        content = []
        with open(tanuki_file, "r") as pf:
            content = pf.readlines()
        with open(tanuki_file, "w") as pf:
            replaced = False
            i = 0
            while i < len(content):
                line = content[i]
                if not replaced:
                    result = re.sub(PATTERN_TANUKI, "/usr/share/corfu/lib/cmdlets-" + new_version + "-shaded.jar", line)
                    if result != line:
                        content[i] = result
                        replaced = True
                i += 1
            pf.writelines(content)
        # check_call("sed -i -e \'s/cmdlets-[0-9.]\+-shaded.jar/cmdlets-" + new_version + "-shaded.jar/\' " + tanuki_file, shell=True)


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--major", type=int, required=False,
                            help="Major version number of CorfuDB.")
    arg_parser.add_argument("--minor", type=int, required=False,
                            help="Minor version number of CorfuDB.")
    arg_parser.add_argument("--patch", type=int, required=False,
                            help="Patch version number of CorfuDB.")
    arg_parser.add_argument("--print-version", action="store_true", required=False,
                            help="Print the current CorfuDB version.")
    arg_parser.add_argument("--set-version", type=str, required=False,
                            help="Set the CorfuDB version.")
    args = arg_parser.parse_args()
    if args.print_version:
        print(version_number_to_string(verify_and_get_version_number()))
        sys.exit(0)
    if args.set_version:
        set_version_number(args.set_version)
        sys.exit(0)
    if args.major == None and args.minor == None and args.patch == None:
        update_version_number()
    elif args.major == None or args.minor == None or args.patch == None:
        print("[ERROR] All major, minor and patch number should be defined!")
        sys.exit(-1)
    else:
        update_version_number([args.major, args.minor, args.patch])