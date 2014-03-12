CORFU
=====

CORFU is a distributed log service. 
Clients link with a client-side library, `com.microsoft.corfu.CorfuClientImpl.java`, 
which implements a simple API (see `com.microsoft.corfu.CorfuExtendedInterface.java`, 
    which extends `com.microsoft.corfu.CorfuInterface.java`).

the `test` folder contains several useful examples:
1. `Helloworld.java`  - walks you through starting a CORFU client, two appends (of different size extents) followed by two reads. 
2. `CorfuAppendTputTester.java` - a customizable append-loop
3. `CorfuReadTputTester.java` - a customizable read-loop
4. `CorfuRWTputTester.java` - a customizable multi-threaded client, with a mix of read/append threads
5. `CorfuShell` - a (**very primitive**) interactive debugger. It lets you manually append entries to the log and read the meta-information back from the log.

=============================================================== 
Bringing up a Corfu service:
==============================================================

The CORFU log is striped over a cluster of storage-units, and employs a sequencer. You must specify an initial configuration for the clients to find the CORFU service. The CORFU library looks for a file named `corfu.xml` in the working directory.

The `scripts` directory contains useful resources for deployment:

1. example `corfu.xml` file
2. example `simplelogger.properties` for controling your logging options
3. `runcorfu.ps1` is a PowerShell script which deploys CORFU servers and sequencer off the configuration


Each storage unit is started by running

>    java com.microsoft.corfu.sunit.CorfuUnitServerImpl -unit <unit num> -drivename <drivename> [-recover]

Another option is to run in-memory, without persistence:

>	java com.microsoft.corfu.sunit.CorfuUnitServerImpl -unit <unit num> <-rammode>
       
the sequencer is run by

>	java com.microsoft.corfu.sequencer.CorfuSequencerImpl


========
Eclipse installation guidelines:    
================================================================
CORFU uses maven for building, simply type on a command line: 
> mvn install 

To import CORFU into Eclipse do:

- File -> Import -> Genereal -> Existing Project into Workspace -> Next
- point the file-browser to the root of the CORFU hierarchy (where `.project `sits).

(Eclipse should then be able to automatically build CORFU.)
