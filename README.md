# CorfuDB
[![Build Status](https://travis-ci.org/CorfuDB/CorfuDB.svg?branch=master)](https://travis-ci.org/CorfuDB/CorfuDB)

CorfuDB is a consistency platform designed around the abstraction
of a shared log. CorfuDB objects are in-memory, highly available
data structures providing linearizable read/write operations and
strictly serializable transactions. CorfuDB is based on
peer-reviewed research published at [SOSP](#references),
[TOCS](#references), and [NSDI](#references).

CorfuDB consists of two layers: a logging layer
which implements a distributed, fault-tolerant shared log; and a
runtime layer that implements transactional services over the shared log.

## Prerequisites
Currently we support and regularly test CorfuDB on Linux (Ubuntu), and
Mac OS X. CorfuDB should also run on Windows as well, but the scripts
are not 100% there yet (pull requests welcome).

To build and run CorfuDB, you will need the Java JDK as well as Apache
Thrift and Maven.

On Linux (Debian/Ubuntu), run:
```
$ sudo apt-get install openjdk-7-jdk maven thrift-compiler
```

On Mac OS X, the [homebrew](http://brew.sh) package manager should help.
After installing homebrew, run:
```
$ brew install maven thrift
```

On Windows, you can get thrift here:
http://www.apache.org/dyn/closer.cgi?path=/thrift/0.9.2/thrift-0.9.2.exe
You can run it directly, but you'll need to add it to your path to play nicely 
with maven; assuming you're using cygwin, you'll need to create an alias for
it so that command line calls to 'thrift' do the right thing. 

## Building CorfuDB

CorfuDB uses Apache maven for building. To build, from the root
directory, run:

```
$ mvn install
```

## CorfuDB quick deployment

The default configuration files will start a single-node deployment
of corfuDB. To start this default deployment, run:

```
$ bin/corfuDBsingle.sh start
```

You may have to run the script as superuser (i.e., with `sudo`).
To stop the deployment, simply call the script with `stop` as
an argument. Other options include `restart` and `status`.

## Checking if the deployment is working

You will probably want to test if your deployment is working. The
`bin/corfuDBTest.sh` script provides an easy way to access built-in
tests and examples. A very simple test is called CorfuHello.
To call it, run:

```
$ bin/corfuDBTest.sh CorfuHello <master-address>
```

Where `<master-address>` is the full address of the master, for example,
http://localhost:8002/corfu.

## Bringing up a custom CorfuDB deployment

A CorfuDB deployment requires a Sequencer, one or more logging units,
and a configuration master.

*Sequencer* - provides unique sequence numbers to clients. Only one
sequencer is required per deployment.

*Logging Units* - provides the actual storage for the log. Several of
these may exist in a deployment.

*Configuration Master* - provides the configuration to clients. Only
one of these may exist per deployment.

Each service uses a configuration file located in the `conf` directory.
Sample configurations for each service type are provided, you will
probably want to modify these for your needs.

The `bin/corfuDBLaunch.sh` script is used to launch each service
configuration. The script takes the following syntax:

`bin/corfuDBLaunch.sh <configuration> {start|stop|restart|status}`

For example, running:

`bin/corfuDBLaunch.sh sequencer start`

Will look for a file `conf/sequencer.yml`, and start the service.

For example, running the simple deployment script
`bin/corfuDBsingle.sh start` runs:

```
$ bin/corfuDBLaunch.sh sequencer start
$ bin/corfuDBLaunch.sh logunit start
$ bin/corfuDBLaunch.sh configmaster start
```

## Deployment Tools

The maven build scripts currently generate Debian packages, which should work
on most Debian-based systems. Furthermore, the [CorfuDB-Ansible](https://github.com/CorfuDB/CorfuDB-Ansible)
repository provides a Ansible playbook to configure, deploy and orchestrate
complex multi-node CorfuDB deployments.

## Common Issues

Q: *I get a bunch of errors that look like*
```
[ERROR] /tmp/CorfuDB/target/generated-sources/thrift/org/corfudb/loggingunit/LogUnitConfigService.java:[2566,7] cannot find symbol
  symbol:   class HashCodeBuilder
  location: class org.corfudb.loggingunit.LogUnitConfigService.rebuild_args
```
*when I run mvn install.*

A: Make sure your version of Thrift matches the version of Thrift in the pom.xml file (currently 0.9.2)

## References

Tango: Distributed Data Structures over a Shared Log.
*Mahesh Balakrishnan, Dahlia Malkhi, Ted Wobber, Ming Wu, Vijayan Prabhakaran,
Michael Wei, John D. Davis, Sriram Rao, Tao Zou, Aviad Zuck.*
SOSP 2013: The 24th ACM Symposium on Operating Systems Principles.

CORFU: A Distributed Shared Log
*Mahesh Balakrishnan, Dahlia Malkhi, Vijayan Prabhakaran, Ted Wobber,
Michael Wei, and John Davis.*
ACM Transactions on Computer Systems (TOCS). December 2013.

CORFU: A Shared Log Design for Flash Clusters.
*Mahesh Balakrishnan, Dahlia Malkhi, Vijayan Prabhakaran, Ted Wobber,
Michael Wei, and John Davis.*
9th USENIX Symposium on Networked Systems Design and Implementation (NSDI '12).
