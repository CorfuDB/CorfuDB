CorfuDB
=====

CorfuDB is a consistency platform designed around the abstraction of a shared log. CorfuDB objects are in-memory, highly available data structures providing linearizable read/write operations and strictly serializable transactions. CorfuDB is based on peer-reviewed research published at SOSP, TOCS, and NSDI. 

CorfuDB consists of two layers: a logging layer in org.corfudb.logging, which implements a distributed, fault-tolerant shared log; and a runtime layer that implements transactional services over the shared log. 

=============================================================== 
Building CorfuDB:
==============================================================

CorfuDB uses maven for building. Simply type on a command line within the CORFULIB directory: 
> mvn install 

=============================================================== 
Bringing up a CorfuDB deployment:
==============================================================

To start a localhost instance of the CorfuDB backend, run the 'rc.py' script in the scripts folder. You can now run CorfuDB clients against this deployment.

(instructions on deploying to a cluster and running tests will follow shortly...)
