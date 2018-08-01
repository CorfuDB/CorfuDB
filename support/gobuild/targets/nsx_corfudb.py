# Copyright 2016 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
NSX-CorfuDB (CorfuDB) gobuild target
"""

import os
import re
import helpers.make
import helpers.target
import helpers.env
from helpers.nsx.chroot import ChrootHelper
from helpers.nsx.dpkg import DebianHelper
import specs.nsx_corfudb
import specs.constants


RELEASENAME = specs.constants.RELEASENAME
MIRROR = specs.constants.UBUNTU_MIRROR
APT_MIRROR = specs.constants.APT_MIRROR
BUILDROOT = "%(buildroot)"
BUILDNUMBER = "%(buildnumber)"
DIST = "xenial_amd64"


class CorfuDBBuild(helpers.target.Target, helpers.make.MakeHelper):
    """
    CorfuDB component
    """
    def GetBuildProductNames(self):
        return {
            "name": "NSX-CORFUDB",
            "longname": "NSX CorfuDB"
            }

    def GetClusterRequirements(self):
        return ['linux-centos64-64']

    def GetRepositories(self, hosttype):
        return [{
            "rcs": "git",
            "src": "corfudb.git;%(branch);",
            "dst": "corfudb"
            }]

    def GetComponentPath(self):
        return "%(buildroot)/publish"

    def GetStorageInfo(self, hosttype):
        return [{
            "type": "source",
            "src": "corfudb"
            }]

    def _GetEnvironment(self, hosttype):
        env = helpers.env.SafeEnvironment(hosttype)
        if hosttype.startswith("linux"):
            lin64prefix = "/build/toolchain/lin64/"
            paths = map((lambda x: lin64prefix + x),
                        ["tar-1.23/bin", "git-1.8.3/bin",
                         "bash-4.1/bin", "python-2.7.6/bin"])
            paths += ["/build/toolchain/lin32/coreutils-8.6/bin"]
            paths += ["/build/toolchain/lin32/rsync-2.6.9/bin"]
            paths += ['/usr/bin', '/bin', '/usr/local/sbin', '/usr/local/bin',
                      '/usr/sbin', '/sbin']
            paths += [env["PATH"]]
            env["PATH"] = os.pathsep.join(paths)
            env["SHELL"] = "/build/toolchain/lin64/bash-4.1/bin/bash"
        return env

    def GetCommands(self, hosttype):
        commands = []
        chroot_dir = os.path.join(BUILDROOT, DIST, 'image')

        buildenv_comp = "%(gobuild_component_nsx_buildenv_root)"
        cmd = "tar -xf %s/%s.tar.gz" % (buildenv_comp, DIST)
        commands.append({
            "desc": "Checkout buildenv: %s" % DIST,
            "root": BUILDROOT,
            "log": "gobuild_checkout_buildenv.log",
            "command": cmd,
            "env": self._GetEnvironment(hosttype)
        })

        cmd = "cp -r corfudb %s/root/" % chroot_dir
        commands.append({
            "desc": "Copying source into buildenv %s" % DIST,
            "root": BUILDROOT,
            "log": "gobuild_copy_source.log",
            "command": cmd,
            "env": self._GetEnvironment(hosttype)
        })

        component_paths = [
            '/build/toolchain/noarch/apache-maven-3.3.3',
        ]
        cmds = []
        for path in component_paths:
            cmds.append("mkdir -p %s/%s" % (chroot_dir, path))
            cmds.append("cp -R %s/* %s/%s" % (path, chroot_dir, path))
        cmd = "; ".join(cmds)

        if component_paths:
            commands.append({
                "desc": "Copy components into buildenv",
                "root": BUILDROOT,
                "log": "buildenv_copy_deps.log",
                "command": cmd,
                "env": self._GetEnvironment(hosttype)
            })

        cmd = "corfudb/support/gobuild/scripts/buildenv applymeta %s" % DIST
        commands.append({
            "desc": "Applying metadata on buildenv %s" % DIST,
            "root": BUILDROOT,
            "log": "gobuild_apply_metadata.log",
            "command": cmd,
            "env": self._GetEnvironment(hosttype)
        })

        cmd = "mkdir -p /proc; mount -t proc none /proc"
        commands.append({
            "desc": "Mount /proc",
            "root": BUILDROOT,
            "log": "gobuild_mount_proc.log",
            "command": ChrootHelper.chroot_cmd(cmd, chroot_dir),
            "env": self._GetEnvironment(hosttype)
        })

        suite, arch = DIST.split("_")
        components = "main restricted multiverse universe"
        cmds = [
            DebianHelper.apt_sources_clean(),
            DebianHelper.apt_sources_add(MIRROR, suite, components=components),
            DebianHelper.apt_sources_add(APT_MIRROR, "%s/%s/" % (suite, arch)),
            DebianHelper.apt_sources_add(APT_MIRROR, "%s/all/" % suite),
            DebianHelper.apt_update()
            ]
        cmd = "; ".join(cmds)
        commands.append({
            "desc": "Setting up apt repositories",
            "root": BUILDROOT,
            "log": "gobuild_apt_update.log",
            "command": ChrootHelper.chroot_cmd(cmd, chroot_dir),
            "env": self._GetEnvironment(hosttype)
        })

        cmd = DebianHelper.apt_install(["devscripts", "equivs", "apt-utils"])
        commands.append({
            "desc": "Install devscripts, equivs, and apt-utils",
            "root": "%(buildroot)",
            "log": "gobuild_apt_install.log",
            "command": ChrootHelper.chroot_cmd(cmd, chroot_dir),
            "env": self._GetEnvironment(hosttype)
        })

        cmd = DebianHelper.apt_install(["oracle-java8-jdk"])
        commands.append({
            "desc": "Install java8 jdk",
            "root": "%(buildroot)",
            "log": "java8_jdk_apt_install.log",
            "command": ChrootHelper.chroot_cmd(cmd, chroot_dir),
            "env": self._GetEnvironment(hosttype)
        })

        cmds = [
            "cd /root/corfudb",
            "/build/toolchain/noarch/apache-maven-3.3.3/bin/mvn clean deploy -DskipTests -DskipITs",
            "mkdir -p /tmp/mvn",
            "mkdir -p /tmp/%s" % (DIST),
            "cp /root/corfudb/debian/target/*.deb /tmp/%s" % (DIST),
            "cp /root/corfudb/migration/target/migration-*-shaded.jar /tmp"
        ]
        # For now, hard-code the sub-module names.
        sub_modules = [
            ".",
            "annotationProcessor",
            "annotations",
            "cmdlets",
            "debian",
            "format",
            "infrastructure",
            "runtime",
            "test"]
        artifact_paths = [os.path.join("/root/corfudb",
                                       module,
                                       "target",
                                       "mvn-repo",
                                       "*")
                          for module in sub_modules]
        cp_cmds = ["cp -r {0} /tmp/mvn".format(path) for path in artifact_paths]
        cmd = " && ".join(cmds + cp_cmds)
        commands.append({
            "desc": "Running mvn clean deploy",
            "root": "%(buildroot)",
            "log": "mvn_clean_deploy.log",
            "command": ChrootHelper.chroot_cmd(cmd, chroot_dir),
            "env": self._GetEnvironment(hosttype)
        })

        publish_dir = os.path.join(BUILDROOT, "publish")
        publish_dist_dir = "%s/%s" % (publish_dir, DIST)
        cmds = ["mkdir -p %s" % publish_dist_dir,
                "mkdir -p %s/mvn" % publish_dir,
                "cp -r %s/tmp/mvn %s" % (chroot_dir, publish_dir),
                "cp %s/tmp/%s/* %s" % (chroot_dir, DIST, publish_dist_dir),
                "cp %s/tmp/migration-*-shaded.jar %s/corfu-data-migration.jar" % (chroot_dir, publish_dir)]
        cmd = " && ".join(cmds)
        commands.append({
            "desc": "Copy build collateral to publish directory",
            "root": BUILDROOT,
            "log": "gobuild_publish.log",
            "command": cmd,
            "env": self._GetEnvironment(hosttype)
        })
        return commands

    # Hack: reading the version and build number from pom.xml
    def GetBuildProductVersion(self, hosttype):
        vfile_path = "%s/corfudb/pom.xml" % self.options.get('buildroot')
        pom = open(vfile_path)
        s = re.search(r'<version>(\S*)</version>', pom.read())
        pom.close()
        return s.group(1)

    def GetComponentDependencies(self):
        comps = {}
        comps["nsx-buildenv"] = {
            "branch": specs.nsx_corfudb.NSX_BUILDENV_BRANCH,
            "change": specs.nsx_corfudb.NSX_BUILDENV_CLN,
            "buildtype": specs.nsx_corfudb.NSX_BUILDENV_BUILDTYPE,
            "files": specs.nsx_corfudb.NSX_BUILDENV_FILES
        }
        return comps
