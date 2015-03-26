package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
import java.io.IOException;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.BundleEntry;

import java.io.Serializable;
import java.util.UUID;

public class StreamObjectServer {

    private static final Logger log = LoggerFactory.getLogger(StreamObjectServer.class);

    public static class StreamObjectWrapper implements Serializable {
        public int serverNum;

        public StreamObjectWrapper(int serverNum)
        {
            this.serverNum = serverNum;
        }
    }

    public static class StreamClientObjectWrapper implements Serializable {
        public int serverNum;

        public StreamClientObjectWrapper(int serverNum)
        {
            this.serverNum = serverNum;
        }
    }

    public static Server server;
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
            final int numthreads = 1;

            CorfuDBClient client = new CorfuDBClient("http://localhost:8002/corfu");
            client.startViewManager();
            final int serverNum = Integer.parseInt(args[0]);
            Stream s = new Stream(client, new UUID(0,serverNum));
            while (true)
            {
                CorfuDBStreamEntry cdse = s.readNextEntry();
                if (cdse instanceof BundleEntry)
                {
                    BundleEntry b = (BundleEntry) cdse;
                    b.writeSlot(null);
                }
            }
    }
}

