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
    def apt_sources_clean(cls, sources_list=None):
        if not sources_list:
            sources_list = "/etc/apt/sources.list"
        cmd = "rm -f %s && touch %s" % (sources_list, sources_list)

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
    def apt_install(cls, packages_list, extra_options=None):
        if extra_options is None:
            extra_options = "--yes --force-yes"
        packages = " ".join(packages_list)

        return "apt-get install %s %s" % (packages, extra_options)
