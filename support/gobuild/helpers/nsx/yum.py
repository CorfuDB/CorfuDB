class YumHelper(object):
    @classmethod
    def add_yum_repo(cls, url, gpgcheck=False):
        if not gpgcheck:
            return "yum-config-manager --nogpgcheck --add-repo %s" % url

    @classmethod
    def yum_install(cls, pkg_list):
        return "yum -y install %s" % " ".join(pkg_list)

    @classmethod
    def yum_install_builddeps(cls, srpm):
        return "yum-builddep -y %s" % srpm
