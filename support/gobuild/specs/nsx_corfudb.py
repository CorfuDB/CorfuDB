import constants

BRANCH = constants.BRANCH

NSX_BUILDENV_BRANCH = 'master'
NSX_BUILDENV_CLN = 'd854a3dc23dd2197870e97e3a3940c5f8db7d66f'
NSX_BUILDENV_FILES = { 'linux-centos64-64': [ r'publish/.*' ] }
NSX_BUILDENV_BUILDTYPE = 'beta'

NSBU_REPOS_BRANCH = BRANCH
NSBU_REPOS_CLN = 'f87370bd6542f456fd142f72a48318fdca544c48'
NSBU_REPOS_BUILDTYPE = 'release'
NSBU_REPOS_FILES = {
    'linux-centos64-64': [r'publish/default/.*']}
