CORFU
=====

CORFU is a distributed log service. 
Clients link with a client-side library, com.microsoft.corfuCorfuClientImpl.java, 
which implements a simple API (see com.microsoft.corfu.CorfuExtendedInterface.java, 
    which extends com.microsoft.corfu.CorfuInterface.java).
    
There is a simple Helloworld.java example in the unittests folder.
It walks you through starting a CORFU client and doing simple log writing/reading.
There are other simple examples under unittests, like WriteTester.java (writer-loop) and ReaderTester.java (reader loop).

There is also a very primitive interactive debugger, com.microsoft.corfu.unittests.CorfuDBG . It lets you manually append entries to the look and read the meta-information back from the log.

=============================================================== 
Bringing up Corfu:
==============================================================

The CORFU log is striped over a cluster of storage-units, and employs one a sequencer. The configuration is described
in file named 0.aux. 0.aux is an XML file; you may look at scripts/0.aux for an example. It is pretty self explanatory.


**To run corfu, you must have the file 0.aux in your classpath**.

**The file and simplelogger.properties in your classpath controls your logging options**.


Each storage unit is started by running
       java com.microsoft.corfu.sunit.CorfuUnitServerImpl -unit <unit #> <-rammode | -drivename <drivename> [-recover]>
       
the sequencer is run by
	java com.microsoft.corfu.sequencer.CorfuSequencerImpl

The file scripts/runcorfu.ps1 contains a powershell script that automatically deploys corfu,
based on the configuration description in 0.aux . Run 'runcorfu.ps1 -push' to make sure any updates you introduce
to binaries or to 0.aux are copied to all of the deployed machines.  

========
Installation guidelines:    
================================================================
Creating an Eclipse project with existing CORFU distribution folder:

File -> New -> Java Project
choose meaningful project name in the box
unclick "Use default location"
in the Location box, type the root of the CORFU distribution folder
(Eclipse will automatically figure out project layout according to folder hierarchy.)

### Now it's time to point Eclipse to jar-files needed by Corfu:####

Right click the project root 
press Build -> Configure Build Path
In the Libraries tab, choose Add External Jars
navigate to the Thrift installation root, and select lib/java/build/libthrift-<YOURVERSION>.jar and 
lib/java/build/lib/*.jar

Do this again with the slf4j installation root, and select slf4j-simple-<YOURVERSION>.jar and slf4j-api-<YOURVERSION>.jar

#### Installing Ant: #####

- download Apache Ant to a directory $ANTHOME
-	add $ANTHOME\bin to environment path

#### Installing Thrift: ####

1) download Apache Thrift and extract into a directory $THRIFTHOME (e.g., c:\Program Files (x86)\thrift-1.9.0)

note, the distribution is in a .tar.gz gzipped-archive. there are a number of free utilities you may use to extract
  the thrift distribution from this archive. if you have cygwin installed, use .. ; 
  otherwise, you may download 7-zip

go to $THRIFTHOME\lib\java, type ‘ant’, and wait for it to build


2) Download the Windows Thrift compiler from here to a location like C:\Program Files (x86)\thrift-0.9.0.exe , and make sure the directory is in your path

########### Installing slf4j: ######

download slf4j and extract into a directory $SLF4JHOME (e.g., c:\Program Files (x86)\slf4j-1.7.5)
