# ![logo](https://github.com/CorfuDB/CorfuDB/blob/master/resources/corfu.png "Corfu")               

[![Join the chat at https://gitter.im/CorfuDB/CorfuDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/CorfuDB/CorfuDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/CorfuDB/CorfuDB.svg?branch=master)](https://travis-ci.org/CorfuDB/CorfuDB) [![Coverage Status](https://coveralls.io/repos/CorfuDB/CorfuDB/badge.svg?branch=master)](https://coveralls.io/r/CorfuDB/CorfuDB?branch=master) 
[![Stories in Ready](https://badge.waffle.io/CorfuDB/CorfuDB.png?label=ready&title=Ready)](https://waffle.io/CorfuDB/CorfuDB)

[![the Corfu Wiki](https://github.com/CorfuDB/CorfuDB/wiki)](https://github.com/CorfuDB/CorfuDB/wiki) 


CorfuDB is a consistency platform designed around the abstraction
of a shared log. CorfuDB objects are in-memory, highly available
data structures providing linearizable read/write operations and
strictly serializable transactions. CorfuDB is based on
peer-reviewed research, see [References](https://github.com/CorfuDB/CorfuDB/wiki/White-papers). 

CorfuDB consists of two layers: a logging layer
which implements a distributed, fault-tolerant shared log; and a
runtime layer that implements transactional services over the shared log.
Check the CorfuDB [wiki](https://github.com/CorfuDB/CorfuDB/wiki) for a detailed overview of the sofware architecture and example usage.

## Prerequisites
Currently we support and regularly test CorfuDB on Linux (Ubuntu), and
Mac OS X. CorfuDB should also run on Windows as well, but the scripts
are not 100% there yet (pull requests welcome).

To build and run CorfuDB, you will need the Java JDK 8 as well as Apache
Thrift, Redis and Maven.

On Linux (Debian/Ubuntu), run:
```
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer maven git
$ wget http://people.apache.org/~jfarrell/thrift/0.9.2/contrib/ubuntu/thrift-compiler_0.9.2_amd64.deb -O /tmp/thrift-compiler.deb
$ sudo dpkg -i /tmp/thrift-compiler.deb
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
$ mvn clean install -DskipTests -Dexec.skip

```

## CorfuDB quick deployment

The default configuration files will start a single-node deployment
of corfuDB. To start this default deployment, run:

```
$ bin/crunall.sh --cmd start
```

You may have to run the script as superuser (i.e., with `sudo`).
To stop the deployment, simply call the script with `--cmd stop` as
an argument. Other options include `--cmd restart` and `--cmd status`. to run a replicated log with `n` copies on a single node, use:

```
$ bin/crunall.sh --cmd start --unitcnt n
```

## Checking if the deployment is working

You will probably want to test if your deployment is working. The class org.corfudb.samples.HelloCorfu performs a few basic "health tests", such connecting with the config-master and retrieving the configuration from it; connecting with the sequencer and retrieving the current tail of the log; and connecting with each one of the logging-units.

To run is manually, either invoke within your Java IDE, or use:
```
$ java -classpath <shaded-jar> org.corfudb.samples.HelloCorfu <master-URL>
```
Where `<shaded-jar>` is the target shaded jar file, for example, target/corfudb-0.1-SNAPSHOT-shaded.jar , and `<master-URL>` is the full address of the master, for example,
http://localhost:8000/corfu.

## Where is my output?

At some point, you may run into problems or error, and you might want to look at output from CorfuDB. You will find various logs under /var/log/corfudb.<rolename>.log , where <rolename> is
one of sequencer, logunit, configmaster.

## Bringing up a custom CorfuDB deployment

The `crunall.sh` single node deployment brings up all the components of a single corfu log. They are:

*Sequencer* - provides unique sequence numbers to clients. Only one
sequencer is required per deployment.

*Logging Units* - provides the actual storage for the log. Several of
these may exist in a deployment.

*Configuration Master* - provides the configuration to clients. Only
one of these may exist per deployment.

Each corfudb role needs a configuration file that contains its port number and other essential parameters. 

The single-node deployment script `crunall.sh` generates configuration files for all roles under the `/var/tmp` directory. Sample configuration files are provided in the `conf` directory and may be changed manually.

The `bin/crun.sh` script is used to launch each component. The script takes the following syntax:

`bin/crun.sh --unit {sequencer|configmaster|logunit.port> --cmd {start|stop|restart|status} [--configdir <dir>]`

For example, running:

`bin/corfuDBLaunch.sh --unit sequencer --cmd start`

Will look for a file `/var/tmp/sequencer.yml`, and start the service.

For example, running the simple deployment script
`bin/crunall.sh start` runs:

```
$ bin/crun.sh --unit sequencer --cmd start
$ bin/crun.sh -unit logunit --cmd start
$ bin/crun.sh --unit configmaster --cmd start
```

## Deployment Tools

The maven build scripts currently generate Debian packages, which should work
on most Debian-based systems. Furthermore, the [CorfuDB-Ansible](https://github.com/CorfuDB/CorfuDB-Ansible)
repository provides a Ansible playbook to configure, deploy and orchestrate
complex multi-node CorfuDB deployments.

## Running the tests

The repository contains a set of unit tests and integration tests that we run CorfuDB against.
To run the tests, you will need to install some additional dependencies - currently this is only
Redis.

To install on Ubuntu:
```
$ sudo apt-get install redis-server
```

On Mac OSX:
```
$ brew install redis
```
## Common Issues

Q: *I get a bunch of errors that look like*
```
[ERROR] /tmp/CorfuDB/target/generated-sources/thrift/org/corfudb/loggingunit/LogUnitConfigService.java:[2566,7] cannot find symbol
  symbol:   class HashCodeBuilder
  location: class org.corfudb.loggingunit.LogUnitConfigService.rebuild_args
```
*when I run mvn install.*

A: Make sure your version of Thrift matches the version of Thrift in the pom.xml file (currently 0.9.2)

## [References](https://github.com/CorfuDB/CorfuDB/wiki/White-papers).
