# ![logo](https://github.com/CorfuDB/CorfuDB/blob/master/resources/corfu.png "Corfu")               

[![Join the chat at https://gitter.im/CorfuDB/CorfuDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/CorfuDB/CorfuDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/CorfuDB/CorfuDB.svg?branch=master)](https://travis-ci.org/CorfuDB/CorfuDB) [![Coverage Status](https://coveralls.io/repos/CorfuDB/CorfuDB/badge.svg?branch=master)](https://coveralls.io/r/CorfuDB/CorfuDB?branch=master) 
[![Stories in Ready](https://badge.waffle.io/CorfuDB/CorfuDB.png?label=ready&title=Ready)](https://waffle.io/CorfuDB/CorfuDB)


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

The Corfu infrastructure consists of three components: a **layout server**, which helps Corfu clients locate the rest of the Corfu infrastructure, a **sequencer server**, which is used to order updates to the log, and a **log server**, which stores updates to the log. At minimum a Corfu infrastructure must have one of each server type, but for scalability and high availability a real-world deployment will have many. An administrator need not worry about installing each role seperately as they are provided as a single monolithic binary.

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
To build Corfu, you will need the Java JDK 8 as well as Apache Maven to invoke the build system.

On Linux (Debian/Ubuntu), run:
```bash
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer maven
```

On Mac OS X, the [homebrew](http://brew.sh) package manager should help.
After installing homebrew, run:
```
$ brew install maven 
```

Once you've installed the prerequisites, you can build Corfu.

```
$ mvn clean install
```

The binaries which will be referenced in the following sections will be located in the ```bin``` directory.

### Running Corfu for the first time

The Corfu infrastructure is provided by the monolithic binary ```corfu_server```. For testing purposes, you will want to run the server in in-memory, single-server mode. To do this, run:

```
$ corfu_server -ms 9000
```

This starts a in-memory single node Corfu infrastructure on port 9000. To point clients at this infrastructure, point them at localhost:9000.

### How do I make sure it works?
To test your Corfu infrastructure, you can use the Corfu utilities. One of the first things you might want to do is check is the layout, which described the configuration of servers in the Corfu infrastructure. To run this, try:

```
$ corfu_layouts -c localhost:9000 query
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
$ echo "hello world" | corfu_stream write -c localhost:9000 -i test
```

This utility takes input from stdin and writes it into the log. This command invocation writes a entry named "hello world" to a stream called "test". Streams are a kind of virtualized log in Corfu - think of them as append-only files.

Next, we can try reading back that stream. This can be done by running:
```
$ corfu_stream read -c localhost:9000 -i test
```
The utility should print back "hello world".

### Cool, it works! But how do I make it distributed?

Now that you have a working Corfu deployment, you'll probably want to make it distributed.

Let's start by adding a layout server. To do that, start a non-provisioned Corfu server instance in addition to the previous one. We'll start it on port 9001.
```
$ corfu_server -m 9001
```

Now lets add that layout server to the previous deployment:
```
$ corfu_layouts -c localhost:9000 edit
```

This should bring up your editor. If you modify the layoutServers line to read:
```json
  "layoutServers": [
    "localhost:9000", "localhost:9001"
  ],
```
This will install that server as a new layout server.

If you check the current layout using the query command:
```
$ corfu_layouts query -c localhost:9000,localhost:9001
```
You will see that you now have two servers in the layout.

How about adding an additional replica so we can tolerate a 
single log server failure? We can use the same ```corfu_server``` 
on port 9001. Recall that ```corfu_server``` is a monolithic binary 
containing all servers. Since we have not yet pointed to the log server 
on 9001, that server has started but is not in use.

To add a replica, we edit the layout again:
```
$ corfu_layouts edit -c localhost:9000,localhost:9001
```

You'll want to add localhost:9001 as a new logunit to the existing segment:
```json
      "logServers": [
        "localhost:9000",
        "localhost:9001"
      ]
```
This adds the log unit at localhost:9001 to the only segment in the system.
to learn more about segments, see the [Corfu wiki](https://github.com/CorfuDB/CorfuDB/wiki).

To scale Corfu, we add additional ``stripes''. To add an additional stripe, 
start a new ```corfu_server``` on port 9002 and edit the layout again:
```
$ corfu_layouts edit -c localhost:9000,localhost:9001
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
                .setType(SMRMap.class)
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

## [References](https://github.com/CorfuDB/CorfuDB/wiki/White-papers).
