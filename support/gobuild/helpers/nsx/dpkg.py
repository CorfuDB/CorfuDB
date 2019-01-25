import os
import specs.constants

from helpers.nsx.chroot import ChrootHelper

MIRROR = specs.constants.UBUNTU_MIRROR

class DebianHelper(object):
    @classmethod
    def apt_update(cls, extra_options=None):
        cmd = "apt-get %s update"
        if extra_options:
            cmd = cmd % extra_options
        else:
            cmd = cmd % ''

        return cmd

    @classmethod
    def apt_sources_clean(cls):
        sources_list = "/etc/apt/sources.list"
        cmd = "rm -f %s && touch %s" % (sources_list, sources_list)
        cmd += "; rm -f /etc/apt/sources.list.d/*"

        return cmd

    @classmethod
    def apt_sources_add(cls, url, dist, components=None, sources_list=None):
        if not sources_list:
            sources_list = "/etc/apt/sources.list"
        if components:
            cmd = "echo \"deb %s %s %s\" >> %s" % (url, dist, components,
                                                   sources_list)
        else:
            cmd = "echo \"deb %s %s\" >> %s" % (url, dist, sources_list)

        return cmd

    @classmethod
    def apt_sources_add_file(cls, sources_list, chroot_dir):
        return ("sudo cp %s %s/etc/apt/sources.list.d"
                % (sources_list, chroot_dir))

    @classmethod
    def apt_install(cls, packages_list, extra_options=None):
        if extra_options is None:
            extra_options = "--yes --force-yes"
        packages = " ".join(packages_list)

        return "apt-get install %s %s" % (packages, extra_options)

    @classmethod
    def setup_with_nsbu_repos(cls, suite, chroot_dir, apt_update=True, entries=[], source_list_files=[]):
        """ New method using using nsbu-repos build and artifactory
            entries: list of sources.list entries
            source_list_files: list of *.list files outside of chroot to copy
                               into chroot's /etc/apt/sources.list.d/
        """

        # Add entries to /etc/apt/sources.list
        components = "main restricted multiverse universe"
        cmds = []
        cmds.append(cls.apt_sources_clean())
        cmds.append(cls.apt_sources_add(MIRROR, suite, components=components))
        cmds.append(cls.apt_sources_add(MIRROR, "%s-updates" % suite,
                                        components=components))
        cmds.append(cls.apt_sources_add(MIRROR, "%s-security" % suite,
                                        components=components))

        for entry in entries:
            cmds.append("echo \"%s\" >> /etc/apt/sources.list" % entry)

        add_entries = ChrootHelper.chroot_cmd("; ".join(cmds), chroot_dir)

        # Copy apt sources.list files to /etc/apt/sources.list.d
        cmds = []
        nsbu_repos = os.path.join("%(gobuild_component_nsbu_repos_root)",
                                  "default/nsbu-%s.list" % suite)
        cmds.append(cls.apt_sources_add_file(nsbu_repos, chroot_dir))

        for f in source_list_files:
            cmds.append(cls.apt_sources_add_file(f, chroot_dir))

        add_files = "; ".join(cmds)
        cmd = add_entries + "; " + add_files


        # Run apt-get update
        if apt_update:
            cmd = cmd + "; " + ChrootHelper.chroot_cmd(cls.apt_update(), chroot_dir)

        return cmd
