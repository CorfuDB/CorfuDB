import os

class CovHelper(object):
    @classmethod
    def gen_covhtml(cls, covfile, cov_dir):
        cmd = "covhtml -f %s %s/cov_html; " % (covfile, cov_dir)
        cmd += "chmod -R +r %s; " % cov_dir
        cmd += "tar -cf %s/cov_html.tar %s/cov_html;" % (cov_dir, cov_dir)
        cmd += "rm -rf %s/cov_html;" % cov_dir
        return cmd

    @classmethod
    def publish_covfile(cls, chroot_dir, covfile, cov_dir, publish_dir, dist, BUILDROOT=None, src_dir=None):
        cmds = ["cp -R %s/%s/cov_html.tar %s/%s"
                 % (chroot_dir, cov_dir, publish_dir, dist)]
        cmds += ["cp %s/%s %s/%s" % (chroot_dir, covfile, publish_dir, dist)]
        if src_dir is not None:
            dir_name = os.path.dirname(src_dir)
            publish_dir_tmp = os.path.join(BUILDROOT, "publish", dist,
                                           "nsx", dir_name)
            cmds += ["mkdir -p %s" % publish_dir_tmp]
            cmds += ["cp -LR nsx/%s %s" % (src_dir, publish_dir_tmp)]
            cmds += ["find -L %s -type l -delete" % publish_dir_tmp]
        return cmds
