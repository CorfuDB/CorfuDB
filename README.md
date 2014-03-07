CORFU
=====

CORFU is a distributed log service. 
Clients link with a client-side library, `com.microsoft.corfu.CorfuClientImpl.java`, 
which implements a simple API (see `com.microsoft.corfu.CorfuExtendedInterface.java`, 
    which extends `com.microsoft.corfu.CorfuInterface.java`).
    
There is a simple `Helloworld.java` example in the `unittests` folder.
It walks you through starting a CORFU client and doing simple log writing/reading.
There are other simple examples under `unittests`, like `WriteTester.java` (writer-loop) and `ReaderTester.java` (reader loop).

There is also a very primitive interactive debugger, `com.microsoft.corfu.unittests.CorfuDBG` . It lets you manually append entries to the look and read the meta-information back from the log.

=============================================================== 
Bringing up a Corfu service:
==============================================================

The CORFU log is striped over a cluster of storage-units, and employs a sequencer. The configuration is described
in file named 0.aux. 0.aux is an XML file; you may look at scripts/0.aux for an example. It is pretty self explanatory.


To run corfu, you must have the **file 0.aux in your classpath**.

The file and **simplelogger.properties in your classpath** controls your logging options.



Each storage unit is started by running

>    java com.microsoft.corfu.sunit.CorfuUnitServerImpl -unit <unit num> -drivename <drivename> [-recover]

Another option is to run in-memory, without persistence:

>	java com.microsoft.corfu.sunit.CorfuUnitServerImpl -unit <unit num> <-rammode>
       
the sequencer is run by

>	java com.microsoft.corfu.sequencer.CorfuSequencerImpl

The file `scripts/runcorfu.ps1` contains a powershell script that automatically deploys CORFU,
based on the configuration description in `0.aux`.  

========
Eclipse installation guidelines:    
================================================================
CORFU uses maven for building, simply type on a command line: 
> mvn install 

To import CORFU into Eclipse do:

- File -> Import -> Genereal -> Existing Project into Workspace -> Next
- point the file-browser to the root of the CORFU hierarchy (where `.project `sits).

(Eclipse should then be able to automatically build CORFU.)
