# CORFU

CorfuDB is a consistency platform designed around the abstraction
of a shared log. CorfuDB objects are in-memory, highly available
data structures providing linearizable read/write operations and
strictly serializable transactions. CorfuDB is based on
peer-reviewed research published at SOSP[^ref-tango-sosp],
TOCS[^ref-corfu-tocs], and NSDI[^ref-corfu-nsdi].

CorfuDB consists of two layers: a logging layer in org.corfudb.logging,
which implements a distributed, fault-tolerant shared log; and a
runtime layer that implements transactional services over the shared log.

## Prerequisites
Currently we support and regularly test CorfuDB on Linux (Ubuntu), and
Mac OS X. CorfuDB should also run on Windows as well, but the scripts
are not 100% there yet (pull requests welcome).

To build and run CorfuDB, you will need the Java JDK as well as apache
Thrift.

On Linux (Debian/Ubuntu), run:
```
$ sudo apt-get install openjdk-7-jdk maven
```

On Mac OS X, the homebrew[http://brew.sh] package manager should help.
After installing homebrew, run:
```
$ brew install maven
```

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

## References

[^ref-tango-sosp] Tango: Distributed Data Structures over a Shared Log.
**Mahesh Balakrishnan, Dahlia Malkhi, Ted Wobber, Ming Wu, Vijayan Prabhakaran,
Michael Wei, John D. Davis, Sriram Rao, Tao Zou, Aviad Zuck.**
SOSP 2013: The 24th ACM Symposium on Operating Systems Principles.

[^ref-corfu-tocs] CORFU: A Shared Log Design for Flash Clusters.
*Mahesh Balakrishnan, Dahlia Malkhi, Vijayan Prabhakaran, Ted Wobber,
Michael Wei, and John Davis.*
ACM Transactions on Computer Systems (TOCS). December 2013.

[^ref-corfu-nsdi] CORFU: A Shared Log Design for Flash Clusters.
*Mahesh Balakrishnan, Dahlia Malkhi, Vijayan Prabhakaran, Ted Wobber,
Michael Wei, and John Davis.*
9th USENIX Symposium on Networked Systems Design and Implementation (NSDI '12).
