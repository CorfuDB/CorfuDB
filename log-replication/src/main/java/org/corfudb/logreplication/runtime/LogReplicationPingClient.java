package org.corfudb.logreplication.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.RuntimeParameters;

@Slf4j
public class LogReplicationPingClient {
    public static void main(String[] args) {
        try {
            System.out.println("Start Client!!!!");
            //LogReplicationRuntime runtime = new LogReplicationRuntime(RuntimeParameters.builder().build());
            //runtime.connect("localhost:9005");
        } catch (Exception e) {
            System.out.println("Error!!!! " + e);
        }
    }
}
