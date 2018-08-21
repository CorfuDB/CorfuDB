package org.corfudb.universe;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleIT {
    private static final Universe UNIVERSE = Universe.getInstance();

    @Test
    public void test(){
        /*VmCluster cluster = UNIVERSE.deployVmCluster(params);

        String localServer = cluster.status().getServers().stream().map(s -> s.getIpAddress()).findFirst().get();
        String connectionString = cluster.status().connectionString();
        assertEquals(localServer, "127.0.0.1");
        assertEquals(connectionString, "localhost:9000,loca");*/
    }
}
