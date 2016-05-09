# Support for automatic downloading of gobuild components during
# developer builds.
#
# IN:
#    GOBUILD_TARGET
#    MAINSRCROOT
#    BUILDROOT
#    OBJDIR
#
# OUT:
#    GOBUILD_AUTO_COMPONENTS_DONE
#    GOBUILD_FOO_ROOT for each component 'foo'

# Guard to ensure this file is only executed at most once per build.
ifndef GOBUILD_AUTO_COMPONENTS_DONE
export GOBUILD_AUTO_COMPONENTS_DONE := 1

ifdef GOBUILD_AUTO_COMPONENTS
# Download components by default
GOBUILD_AUTO_COMPONENTS_DOWNLOAD ?= 1

ifndef GOBUILD_TARGET
$(error GOBUILD_TARGET is not defined.\
Makefiles that include gobuild-auto-components.mk must define GOBUILD_TARGET.)
endif

ifndef MAINSRCROOT
$(error MAINSRCROOT is not defined.\
Makefiles that include gobuild-auto-components.mk must define MAINSRCROOT.)
endif

ifndef BUILDROOT
$(error BUILDROOT is not defined.\
Makefiles that include gobuild-auto-components.mk must define BUILDROOT.)
endif

ifndef OBJDIR
$(error OBJDIR is not defined.\
Makefiles that include gobuild-auto-components.mk must define OBJDIR.)
endif

ifeq ($(OS), Windows_NT)

BUILDAPPS ?= //build-apps/apps/bin
ifdef GOBUILD_AUTO_COMPONENTS_USE_MSYS
GOBUILD_CREATE_SPECINFO_OPTIONS := --msys
GOBUILD_CMD ?= $(BUILDAPPS)/gobuild
GOBUILD_MKDIR := $(TCROOT)/win32/mingw-20111012/msys/1.0/bin/mkdir.exe
GOBUILD_MV := $(TCROOT)/win32/mingw-20111012/msys/1.0/bin/mv.exe
GOBUILD_PYTHON := $(TCROOT)/win32/python-2.6.1/python.exe
else
GOBUILD_CMD ?= $(BUILDAPPS)/gobuild.cmd
GOBUILD_MKDIR := $(TCROOT)/win32/cygwin-1.5.19-4/bin/mkdir.exe
GOBUILD_MV := $(TCROOT)/win32/cygwin-1.5.19-4/bin/mv.exe
GOBUILD_PYTHON := $(TCROOT)/win32/python-2.6.1/python.exe
endif

else

BUILDAPPS ?= /build/apps/bin
GOBUILD_CMD ?= $(BUILDAPPS)/gobuild
ifdef MACOS
GOBUILD_MKDIR := $(TCROOT)/mac32/coreutils-5.97/bin/mkdir
GOBUILD_MV := $(TCROOT)/mac32/coreutils-5.97/bin/mv
GOBUILD_PYTHON := $(TCROOT)/mac32/python-2.6.1/bin/python
else
GOBUILD_MKDIR := $(TCROOT)/lin32/coreutils-5.97/bin/mkdir
GOBUILD_MV := $(TCROOT)/lin32/coreutils-5.97/bin/mv
GOBUILD_PYTHON := $(TCROOT)/lin32/python-2.6.1/bin/python
endif

endif

GOBUILD_LOG_DIR := $(BUILDROOT)/$(OBJDIR)/gobuild/log
GOBUILD_SPECINFO_DIR := $(BUILDROOT)/$(OBJDIR)/gobuild/specinfo
GOBUILD_LOCALCACHE_DIR ?= $(BUILDROOT)/gobuild/compcache

GOBUILD_DEPS ?= $(GOBUILD_CMD) deps
GOBUILD_CREATE_SPECINFO := \
   $(GOBUILD_PYTHON) $(MAINSRCROOT)/support/gobuild/scripts/create-specinfo.py

GOBUILD_SPECINFO_PY ?= $(wildcard $(MAINSRCROOT)/support/gobuild/specs/*.py)
GOBUILD_TARGETS_PY ?= $(wildcard $(MAINSRCROOT)/support/gobuild/targets/*.py)
GOBUILD_SPECINFO_RAW := $(GOBUILD_SPECINFO_DIR)/$(GOBUILD_TARGET).spec
GOBUILD_SPECINFO_MK := $(GOBUILD_SPECINFO_DIR)/$(GOBUILD_TARGET).mk

# Translate the space-separated list of components to skip into a
# comma-separated list to pass to the "gobuild deps" command.
comma := ,
empty :=
space := $(empty) $(empty)
GOBUILD_AUTO_COMPONENTS_SKIP_CS := \
   $(subst $(space),$(comma),$(GOBUILD_AUTO_COMPONENTS_SKIP))

GOBUILD_DEPS_CMD := $(GOBUILD_DEPS)
GOBUILD_DEPS_CMD += $(GOBUILD_TARGET)
GOBUILD_DEPS_CMD += --srcroot=$(MAINSRCROOT)
ifdef BRANCH_NAME
GOBUILD_DEPS_CMD += --branch=$(BRANCH_NAME)
endif
GOBUILD_DEPS_CMD += --buildtype=$(OBJDIR)
GOBUILD_DEPS_CMD += --localcache=$(GOBUILD_LOCALCACHE_DIR)
GOBUILD_DEPS_CMD += --skip.components=$(GOBUILD_AUTO_COMPONENTS_SKIP_CS)
GOBUILD_DEPS_CMD += --logdir=$(GOBUILD_LOG_DIR)
ifdef GOBUILD_AUTO_COMPONENTS_DOWNLOAD
GOBUILD_DEPS_CMD += --download
endif
ifdef GOBUILD_AUTO_COMPONENTS_REQUEST
GOBUILD_DEPS_CMD += --request
endif
ifdef GOBUILD_AUTO_COMPONENTS_WAIT
GOBUILD_DEPS_CMD += --wait
endif
ifdef GOBUILD_AUTO_COMPONENTS_HOSTTYPE
GOBUILD_DEPS_CMD += --hosttype=$(GOBUILD_AUTO_COMPONENTS_HOSTTYPE)
endif
ifdef GOBUILD_AUTO_COMPONENTS_NO_SEND_EMAIL
GOBUILD_DEPS_CMD += --no.send.email
endif

# Rely on GNU make behavior to notice that GOBUILD_SPECINFO_MK is both a
# target and an include, and automatically rebuild it. See Section 3.7 of
# the GNU make manual (How Makefiles Are Remade) for details.
include $(GOBUILD_SPECINFO_MK)

$(GOBUILD_TARGET)-deps: $(PHONY) $(GOBUILD_SPECINFO_MK)

# Run "gobuild deps" to produce a raw spec file containing information
# about the required components. Translate the raw spec file into a
# .mk file suitable for inclusion, which defines an exported variable
# GOBUILD_FOO_ROOT for each component 'foo'. Use intermediate files
# (.new) in order to avoid problems that could occur if the build is
# interrupted in the middle of writing the file.
$(GOBUILD_SPECINFO_MK): $(GOBUILD_SPECINFO_PY) $(GOBUILD_TARGETS_PY) \
	$(GOBUILD_CREATE_SPECINFO)
ifeq ($(wildcard $(GOBUILD_CMD)),)
	$(error Cannot find $(GOBUILD_CMD).\
	This program is needed to download components for the build.\
	In order to build offline, run\
	"make PRODUCT=$(PRODUCT) OBJDIR=$(OBJDIR) $(GOBUILD_TARGET)-deps"\
	prior to disconnecting)
endif
	@echo "** Generating $@"
	@echo "** Please ignore any above error about $@ not existing" 1>&2
	$(GOBUILD_MKDIR) -p $(GOBUILD_LOG_DIR) $(GOBUILD_LOCALCACHE_DIR) $(GOBUILD_SPECINFO_DIR)
	$(GOBUILD_DEPS_CMD) > $(GOBUILD_SPECINFO_RAW).new
	$(GOBUILD_MV) -f $(GOBUILD_SPECINFO_RAW).new $(GOBUILD_SPECINFO_RAW)
	$(GOBUILD_CREATE_SPECINFO) --format=make $(GOBUILD_CREATE_SPECINFO_OPTIONS) < $(GOBUILD_SPECINFO_RAW) > $@.new
	$(GOBUILD_MV) -f $@.new $@
else
# Make sure -deps target is still defined even if we are not using
# auto-component logic.
$(GOBUILD_TARGET)-deps: $(PHONY)
endif

endif
