class ChrootHelper(object):
    @classmethod
    def chroot_cmd(cls, cmd, chroot_dir):
        return "sudo chroot %s /bin/sh -lc \'%s\'" % (chroot_dir, cmd)

    @classmethod
    def add_cleanup(cls, cmd):
        on_failure = [
            'umount -l /proc',
            '/bin/false',
        ]
        on_failure_cmd = '; '.join(on_failure)
        return "%s || (%s)" % (cmd, on_failure_cmd)
