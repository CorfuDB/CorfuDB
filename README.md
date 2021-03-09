# ![logo](https://github.com/CorfuDB/CorfuDB/blob/master/resources/corfu.png "Corfu")

[![Join the chat at https://gitter.im/CorfuDB/CorfuDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/CorfuDB/CorfuDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Github Actions](https://github.com/CorfuDB/CorfuDB/actions/workflows/pull_request.yml/badge.svg)](https://github.com/CorfuDB/CorfuDB/actions)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/48cc2949fabb427fbee7bfca9b872561)](https://www.codacy.com/gh/CorfuDB/CorfuDB/dashboard?utm_source=github.com&utm_medium=referral&utm_content=CorfuDB/CorfuDB&utm_campaign=Badge_Coverage)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/83bcbf63024b4937999c9b0348672abf)](https://app.codacy.com/gh/CorfuDB/CorfuDB?utm_source=github.com&utm_medium=referral&utm_content=CorfuDB/CorfuDB&utm_campaign=Badge_Grade_Settings)

Corfu is a consistency platform designed around the abstraction
of a shared log. CorfuDB objects are in-memory, highly available
data structures providing linearizable read/write operations and
strictly serializable transactions. CorfuDB is based on
peer-reviewed research, see [References](https://github.com/CorfuDB/CorfuDB/wiki/White-papers). 

Check the [Corfu Wiki](https://github.com/CorfuDB/CorfuDB/wiki) for a detailed overview of the 
software architecture and example usage.

### Table of Contents
[System Requirements](#what-do-i-need-to-run-corfu) 

[Corfu Basics](#corfu-basics)

[Corfu Quick Start](#ok-great-get-me-started-running-corfu)

[Developing with Corfu](#now-i-want-to-write-a-program-that-uses-corfu)

## What do I need to run Corfu?
The Corfu infrastructure can run on any system which has Java 8 support. We do not impose any requirements on the kind of storage used - Corfu works with any device that your operating system will allow Java to work with: traditional hard disks, SSDs, and even NVM. We also provide an in-memory mode for nodes which do not require persistence. 

Even though Corfu is a distributed system, you can start working with Corfu using just a single machine. In addition, you can easily simulate a distributed Corfu system on a single machine using just a few commands.

## So how does Corfu work?
Corfu is built upon the abstraction of a distributed shared log. The Corfu infrastructure provides this log to clients, which use the log for coordination, communication and storage. The log is a highly available, dynamic and high performance scalable fabric: it is resilient to failures and can be reconfigured at any time.

The Corfu infrastructure consists of three components: a **layout server**, which helps Corfu clients locate the rest of the Corfu infrastructure, a **sequencer server**, which is used to order updates to the log, and a **log server**, which stores updates to the log. At minimum a Corfu infrastructure must have one of each server type, but for scalability and high availability a real-world deployment will have many. An administrator need not worry about installing each role separately as they are provided as a single monolithic binary.

Corfu clients interact with the infrastructure through the Corfu runtime. The runtime is currently only available in Java, but we plan on providing it in several other languages in the future. Given the address to a layout server in a Corfu infrastructure, the runtime enables clients to access distributed high-level data structures as if they were local data structures. We provide a mechanism to automatically distribute most Java objects as well. 

For more details on the inner workings of Corfu, see the [Corfu wiki](https://github.com/CorfuDB/CorfuDB/wiki).

## Ok, great - get me started running Corfu!

There are currently two ways to run Corfu - by building the development sources, or on Debian-based systems, installing the corfu-server package. We'll describe how to build Corfu from the development sources first. If you just want to install the Debian package, skip [here](#install-from-debian-package).

### Install From Debian Package

We currently host an apt repository for Ubuntu 14.04 LTS (Trusty).
To install Corfu via ```apt-get```, run the following commands:

```bash
# Install the package for add-apt-repository
$ sudo apt-get install python-software-properties
# Add the Corfu signing key to your keychain
$ sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 482CD4B4
# Add the Corfu repository
$ sudo apt-add-repository "deb https://raw.github.com/CorfuDB/Corfu-Repos/debian/ trusty main"
# Update packages and install the Corfu server infrastructure
$ sudo apt-get update
$ sudo apt-get install corfu-server
```

### Building Corfu From Source
To build Corfu, you will need the Java JDK 8 as well as Apache Maven
3.3 or later to invoke the build system.

On Linux (Debian/Ubuntu), run:

```bash
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
```

Your major release number of Debian/Ubuntu will determine whether the
simple command below is sufficient to install Maven 3.3 or later.

```bash
$ sudo apt-get install maven
```

Use the command `mvn --version` to confirm that Maven 3.3 or later is
installed.  If an older version is installed, then use the
instructions at
[Installing maven 3.3 on Ubuntu](https://npatta01.github.io/2015/08/05/maven_install/)
to install manually.
**PLEASE NOTE:** Please substitute the version number `3.3.9` in place of this
blog's instructions for an older & unavailable `3.3.3`.

On Mac OS X, the [homebrew](http://brew.sh) package manager should help.
After installing homebrew, run:
```
$ brew install maven 
```

The OS X package manager [MacPorts](http://macports.org/) can also
install Maven 3 via `sudo port install maven3`.

### Double-check Java and Maven prerequisites

Use the command `mvn --version` to confirm that Maven 3.3 or later is
installed.  Output should look like:

    % mvn --version
    Apache Maven 3.3.9 (bb52d8502b132ec0a5a3f4c09453c07478323dc5; 2015-11-10T08:41:47-08:00)
    Maven home: /opt/local/share/java/maven3
    Java version: 1.8.0_91, vendor: Oracle Corporation
    Java home: /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre
    Default locale: en_US, platform encoding: UTF-8
    OS name: "mac os x", version: "10.11.6", arch: "x86_64", family: "mac"

Some OS X users have had problems where the version of Maven installed
by MacPorts uses a different Java version than expected.  Java version
1.8 or later is required.  If Java 1.7 or earlier is reported, then
refer to this
[StackOverflow Maven JDK mismatch question](http://stackoverflow.com/questions/18813828/why-maven-use-jdk-1-6-but-my-java-version-is-1-7).

### Building Corfu

Once you've installed the prerequisites, you can build Corfu.

```
$ mvn clean install
```

The binaries which will be referenced in the following sections will be located in the ```bin``` directory.

### Running Corfu for the first time

The Corfu infrastructure is provided by the monolithic binary ```corfu_server```. For testing purposes, you will want to run the server in in-memory, single-server mode. To do this, run:

```
$ ./CorfuDB/bin/corfu_server -ms 9000
```

This starts an in-memory single node Corfu infrastructure on port 9000. To point clients at this infrastructure, point them at localhost:9000.

### How do I make sure it works?
To test your Corfu infrastructure, you can use the Corfu utilities. One of the first things you might want to do is check is the layout, which described the configuration of servers in the Corfu infrastructure. To run this, try:

```
$ ./CorfuDB/bin/corfu_layouts -c localhost:9000 query
```

You should get output similar to this:
```json
{
  "layoutServers": [
    "localhost:9000"
  ],
  "sequencers": [
    "localhost:9000"
  ],
  "segments": [
    {
      "replicationMode": "CHAIN_REPLICATION",
      "start": 0,
      "end": -1,
      "stripes": [
        {
          "logServers": [
            "localhost:9000"
          ]
        }
      ]
    }
  ],
  "epoch": 0
}
```

This means that the infrastructure is currently configured with a single layout server, a single sequencer, and a single replica which is replicated using chain replication.

Now we can try writing to the instance. The stream utility can be used to write to the instance:
```
$ echo "hello world" | ./CorfuDB/bin/corfu_stream append -c localhost:9000 -i test
```

This utility takes input from stdin and writes it into the log. This command invocation writes a entry named "hello world" to a stream called "test". Streams are a kind of virtualized log in Corfu - think of them as append-only files.

Next, we can try reading back that stream. This can be done by running:
```
$ ./CorfuDB/bin/corfu_stream read -c localhost:9000 -i test
```
The utility should print back "hello world".

### Cool, it works! But how do I make it distributed?

Now that you have a working Corfu deployment, you'll probably want to make it distributed.

Let's start by adding 2 non-provisioned Corfu server instances. We'll start these on ports 9000 and 9001 respectively.
```
$ ./CorfuDB/bin/corfu_server -m 9000 &
$ ./CorfuDB/bin/corfu_server -m 9001 &
```

Now let's bootstrap these ```corfu_server``` instances into a cluster.
To do that edit the json obtained from the layouts query above in a file called, say layout.json and add in the second server:
```json
{
  "layoutServers": [
    "localhost:9000",
    "localhost:9001"
  ],
  "sequencers": [
    "localhost:9000",
    "localhost:9001"
  ],
  "segments": [
    {
      "replicationMode": "CHAIN_REPLICATION",
      "start": 0,
      "end": -1,
      "stripes": [
        {
          "logServers": [
            "localhost:9000",
            "localhost:9001"
          ]
        }
      ]
    }
  ],
  "epoch": 0
}
```
Note that we are adding the second server in port 9001 as a layoutServer, a sequencer and a logServer all in one.
Once you have edited the file layout.json add it to the cluster using the following command:
```
$ ./CorfuDB/bin/corfu_bootstrap_cluster -l layout.json
```

If you check the current layout using the query command:
```
$ ./CorfuDB/bin/corfu_layouts query -c localhost:9000,localhost:9001
```
You will see that you now have two servers in the layout. Recall that ```corfu_server``` is a monolithic binary
containing all servers. The above layout.json provisions the second server as another replica so the cluster can tolerate
a single failure.

To learn more about segments, see the [Corfu wiki](https://github.com/CorfuDB/CorfuDB/wiki).

To scale Corfu, we add additional ``stripes''. To add an additional stripe, first 
start a new ```corfu_server``` on port 9002:
```
./CorfuDB/bin/corfu_server -m 9002
```
Redeploy this cluster with the following additions to layout.json:
The layoutServers line should read:
```json
  "layoutServers": [
    "localhost:9000", "localhost:9001", "localhost:9002"
  ],
```
This time we add localhost:9002 as a new stripe.

```json
      "stripes": [
        {
          "logServers": [
            "localhost:9000",
            "localhost:9001"
          ]
        },
        {
          "logServers": [
            "localhost:9002"
          ]
        }
      ]
```

This adds the logunit at localhost:9002 as an additional stripe in the system. That is, writes to even addresses will now go to
localhost:9000 and localhost:9001, while writes to odd addresses will go to localhost:9002.

## Now I want to write a program that uses Corfu!

To write your first program that uses Corfu, you will want to add Corfu as a dependency. For Maven-based projects, you can add:

```xml
 <dependency>
    <groupId>org.corfudb</groupId>
    <artifactId>runtime</artifactId>
    <version>0.1-SNAPSHOT</version>
    <scope>compile</scope>
</dependency>
```
to your pom.xml file. 

You will also want to add the Corfu Maven repository, unless you ran ```mvn install``` from source to install the jar files locally:
```xml
<repositories>
    <repository>
        <id>corfu-mvn-repo</id>
        <url>https://raw.github.com/CorfuDB/Corfu-Repos/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>
```

Once you have Corfu added as a dependency, you can start writing Corfu code. Let's start with a map:
```java
    CorfuRuntime rt = new CorfuRuntime("localhost:9000")
                            .connect();

    Map<String,Integer> map = rt.getObjectsView()
                .build()
                .setStreamName("A")
                .setType(CorfuTable.class)
                .open();

    Integer previous = map.get("a");
    if (previous == null) {
        System.out.println("This is the first time we were run!");
        map.put("a", 1);
    }
    else {
        map.put("a", ++previous);
        System.out.println("This is the " + previous + " time we were run!");
    }
```

You can run this code multiple times from many clients, and each client should display a unique "run".


### Documentation

In addition to the github wiki more documentation can be found in the docs folder. An API spec and
class diagrams can be generated by running `doxygen` in the parent directory, the output will be
in docs/doxygen.


## [References](https://github.com/CorfuDB/CorfuDB/wiki/White-papers).
