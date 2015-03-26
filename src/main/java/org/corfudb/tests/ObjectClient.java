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
import com.esotericsoftware.kryonet.Client;

import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.IOException;
public class ObjectClient {

    private static final Logger log = LoggerFactory.getLogger(ObjectClient.class);

    private static ConcurrentHashMap<Integer, ObjectServer.TwoPLRequest> rpcMap = new ConcurrentHashMap<Integer, ObjectServer.TwoPLRequest>();
    private static ConcurrentHashMap<Integer, ObjectServer.TwoPLResponse> rpcResponseMap = new ConcurrentHashMap<Integer, ObjectServer.TwoPLResponse>();

    private static AtomicInteger counter = new AtomicInteger();

    private static ObjectServer.TwoPLRequest generateRequest(ArrayList<Integer> list, boolean read, boolean write, boolean unlock, int key, String value)
    {
        ObjectServer.TwoPLRequest tpr = new ObjectServer.TwoPLRequest();
        tpr.id = counter.getAndIncrement();
        tpr.readLock = read;
        tpr.writeLock = write;
        tpr.unlock = unlock;
        tpr.key = key;
        tpr.value = value;
        rpcMap.put(tpr.id, tpr);
        list.add(tpr.id);
        return tpr;
    }

    private static void waitForRPCs(ArrayList<Integer> ids)
    {
        for (Integer id : ids)
        {
            ObjectServer.TwoPLRequest tpr_orig = rpcMap.get(id);
            try {
            synchronized(tpr_orig)
            {
                tpr_orig.wait();
            }}
            catch(InterruptedException ie) {}
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
           Client c = new Client();
           c.getKryo().register(ObjectServer.TwoPLRequest.class);
           c.getKryo().register(ObjectServer.TwoPLResponse.class);
           new Thread(c).start();
           c.connect(5000, "localhost", 5000, 5000);
           c.addListener( new Listener() {
                public void received(Connection connection, Object obj)
                {
                    if (obj instanceof ObjectServer.TwoPLResponse)
                    {
                        ObjectServer.TwoPLResponse tpr = (ObjectServer.TwoPLResponse) obj;
                        ObjectServer.TwoPLRequest tpr_orig = rpcMap.get(tpr.id);
                        synchronized(tpr_orig)
                        {
                            tpr_orig.notifyAll();
                        }
                        rpcResponseMap.put(tpr.id, tpr);
                        log.info("Response[{}]: {}", tpr.id, tpr.success);
                    }
                }}
            );
           log.info("Phase 1: Acquiring locks");
           ArrayList<Integer> rpcList = new ArrayList<Integer>();
           c.sendTCP(generateRequest(rpcList, false, true, false, 0, "hello world"));
           waitForRPCs(rpcList);
           log.info("Phase 2: Releasing locks");
           rpcList.clear();
           c.sendTCP(generateRequest(rpcList, false, false, true, 0, ""));
           waitForRPCs(rpcList);

    }
}

