# Integration testing in CorfuDB

Integration testing is often a difficult venture, especially when it comes to distributed systems.
This testing framework helps to speed up this process by making it easier to link together services in a cluster.

### The idea
To be defined

### Design:
 - project - integtest is a module of corfu db maven project. 
 - running tests by `mvn clean integration-test`
 - {Definition of the testing framework elements: Universe, Cluster, Service, Node.}
 - {Definition of the testing framework Actions: Add, Kill, Stop, Remove, Deploy ...}
 ...
 
### Lifecycle. `mvn clean integration-test` executes the following steps:
 #### maven:
 - build a corfu `infrastructure` shaded jar (corfu server).
 - copy the infrastructure jar into `integtest/target/corfudb` directory.
 - build a docker image which contains the jar - corfudb-image.
 
 #### Basic integration test:
 - universe build docker image....
 - 
 
### Lifecycle. `mvn clean integration-test` executes the following steps:
 #### maven:
 - build a corfu `infrastructure` shaded jar (corfu server).
 - copy the infrastructure jar into `integtest/target/corfudb` directory.
 - build a docker image which contains the jar - corfudb-image.
 
 #### Basic integration test:
 - create a user network
 - start container using corfudb-image, map docker port to host port
 - create CorfuRuntime and connect to the container
 - work with db
 - check test result
 - delete network, kill container, remove container 

#### === The idea of docker cluster implementation: ===
 - Build a docker image of a corfu server.
 - Use spotify docker-client to declare the needed infrastructure for each test case.
 - Start up the infrastructure with docker-client and execute tests.
 
### Docker cluster advantages:
 - Completely isolated environment for each db instance.
 - Full programmatic control on the container in tests during execution time. 
   Rich docker API allows manage any part of container lifecycle out of the box.
 - You are given your own virtual network. 
   It's quite easy to manipulate nodes (add, delete or exclude it from the network and then join it again). 
   Partitioning can be done easily using docker network api.
 - Easy access to any node logs. Mac has the UI (Kitematic) that makes it even easier.
 - Allows to bring any needed infrastructure services in the test for additional analytics, for instance it could be ELK or Grafana, 
   which makes post analytics easier.
 
### Docker cluster Issues/workarounds:
 - `--network=host` parameter not supported in MacOS, workaround is use fake dns on host
 
### Dockr cluster visualization design

![Alt text](https://goo.gl/kMFBtd)