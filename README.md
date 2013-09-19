CORFU
=====

CORFU is a distributed log service. 
Clients link with a client-side library, com.microsoft.corfuCorfuClientImpl.java, 
which implements a simple API (see com.microsoft.corfu.CorfuExtendedInterface.java, 
    which extends com.microsoft.corfu.CorfuInterface.java).
    
================================================================
Creating an Eclipse project with existing JCORFU distribution folder:

File -> New -> Java Project
choose meaningful project name in the box
unclick "Use default location"
in the Location box, type the root of the JCORFU distribution folder
(Eclipse will automatically figure out project layout according to folder hierarchy.)

==============================================================

Right click the project root 
press Build -> Configure Build Path
In the Libraries tab, choose Add External Jars
navigate to the Thrift installation root, and select lib/java/build/libthrift-<YOURVERSION>.jar and 
lib/java/build/lib/*.jar

=============================================================== 

=====================
Installing Ant:
=====================

- download Apache Ant  from here to a directory $ANTHOME
-	add $ANTHOME\bin to environment path

=====================
Installing Thrift:
=====================

1) download Apache Thrift from here and extract into a directory $THRIFTHOME

note, the distribution is in a .tar.gz gzipped-archive. there are a number of free utilities you may use to extract
  the thrift distribution from this archive. if you have cygwin installed, use .. ; 
  otherwise, you may download 7-zip

go to $THRIFTHOME\lib\java, type ‘ant’, and wait for it to build


2) Download the Windows Thrift compiler from here to a location like C:\Program Files (x86)\thrift-0.9.0.exe , and make sure the directory is in your path

