// package org.corfudb.universe.corfuproxy;
//
// import org.corfudb.universe.Universe;
// import org.corfudb.universe.cluster.docker.DockerCluster;
// import org.corfudb.universe.node.CorfuClient;
// import org.junit.Test;
//
// import java.time.Duration;
//
// public class ProxyIT {
//     private static final Universe UNIVERSE = Universe.getInstance();
//
//     // private final DockerClient docker;
//     private DockerCluster dockerCluster;
//
//     public ProxyIT() throws Exception {
//         // this.docker = DefaultDockerClient.fromEnv().build();
//     }
//
//     // @After
//     // public void tearDown() {
//     //     dockerCluster.shutdown();
//     // }
//
//     @Test
//     public void ClientOperationTest() throws InterruptedException {
//         // Fixtures.ClusterFixture clusterFixture = Fixtures.ClusterFixture.builder().build();
//         // //setup
//         // dockerCluster = UNIVERSE
//         //         .buildDockerCluster(clusterFixture.data(), docker)
//         //         .deploy();
//
//         CorfuClient client = new CorfuClient("localhost", 7001).setup();
//         // client.close();
//         // client.setup();
//
//         System.out.println("Calling addLayoutServer()...");
//         client.addLayoutServer(
//                 "localhost:9000",
//                 resp -> System.out.println("1 Message id: " + resp.getMessageId()));
//
//         Thread.sleep(2000);
//
//         System.out.println("Calling connect()...");
//         client.connect(resp -> System.out.println("2 Message id: " + resp.getMessageId()));
//
//         Thread.sleep(2000);
//
//         System.out.println("Calling addNode()...");
//         client.addNode("localhost:9001", 1, Duration.ofSeconds(1), Duration.ofMillis(50),
//                 resp -> {
//                     System.out.println("3 Message id: " + resp.getMessageId());
//                     if (resp.hasErrorMsg()) {
//                         System.out.println("Error message: " + resp.getErrorMsg());
//                         System.out.println(resp.getStackTrace());
//                     }
//                 });
//
//         Thread.sleep(3000);
//
//         System.out.println("Calling getAllServers()...");
//         client.getAllServers(resp -> {
//             System.out.println("4 Message id: " + resp.getMessageId());
//             System.out.println("All servers:");
//             for (String server : resp.getGetAllServersResponse().getServersList()) {
//                 System.out.println(server);
//             }
//         });
//
//         Thread.sleep(2000);
//
//         System.out.println("Calling removeNode()...");
//         client.removeNode("localhost:9001", 3, Duration.ofMinutes(5), Duration.ofMillis(50),
//                 resp -> System.out.println("5 Message id: " + resp.getMessageId()));
//
//         Thread.sleep(2000);
//
//         System.out.println("Calling invalidateLayout()...");
//         client.invalidateLayout(resp -> System.out.println("6 Message id: " + resp.getMessageId()));
//
//         Thread.sleep(2000);
//
//         System.out.println("Calling getAllActiveServers()...");
//         client.getAllActiveServers(resp -> {
//             System.out.println("7 Message id: " + resp.getMessageId());
//             System.out.println("All active servers:");
//             for (String server : resp.getGetAllActiveServersResponse().getServersList()) {
//                 System.out.println(server);
//             }
//         });
//
//         Thread.sleep(2000);
//     }
//
//
// }
