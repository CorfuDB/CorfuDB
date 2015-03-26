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
public class ObjectServer {

    private static final Logger log = LoggerFactory.getLogger(ObjectServer.class);

    public static class TwoPLObject
    {
        public boolean readLock;
        public boolean writeLock;
        public String data;

        public TwoPLObject()
        {
            readLock = false;
            writeLock = false;
            data = "default";
        }
    }

    public static class TwoPLRequest   {
        public int id;
        public boolean readLock;
        public boolean writeLock;
        public boolean unlock;
        public int key;
        public String value;
        public TwoPLRequest()
        {

        }
    }

    public static class TwoPLResponse {
        public int id;
        public boolean success;
        public String value;

        public TwoPLResponse()
        {

        }
    }

    public static ConcurrentHashMap<Integer, TwoPLObject> hashMap = new ConcurrentHashMap<Integer, TwoPLObject>();
    public static Server server;
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
            server = new Server(16384, 8192);
            int port = 5000;
            server.getKryo().register(TwoPLRequest.class);
            server.getKryo().register(TwoPLResponse.class);
            server.start();
            log.info("Object server bound to TCP/UDP port " + port);
            try {
                server.bind(port, port);
            }
            catch (IOException ie)
            {
                log.debug("Error binding object server", ie);
            }
            server.addListener(new Listener(){
                public void received (Connection connection, Object object)
                {
                    if (object instanceof TwoPLRequest)
                    {
                        TwoPLRequest tpr = (TwoPLRequest) object;
                        TwoPLResponse tpresp = new TwoPLResponse();
                        hashMap.putIfAbsent(tpr.key, new TwoPLObject());
                        TwoPLObject tpo = hashMap.get(tpr.key);
                        boolean success =  false;
                        synchronized(tpo)
                        {
                            tpresp.id = tpr.id;
                            if (tpr.readLock && !tpo.writeLock)
                            {
                                tpo.readLock = true;
                                tpresp.success = true;
                                tpresp.value = tpo.data;
                            }
                            else if (tpr.writeLock && !tpo.readLock && !tpo.writeLock)
                            {
                                tpo.writeLock = true;
                                tpo.data = tpr.value;
                                tpresp.success = true;
                            }
                            else if (tpr.unlock)
                            {
                                tpo.writeLock = false;
                                tpo.readLock = false;
                                tpresp.success = true;
                            }
                            else
                            {
                                tpresp.success = false;
                            }
                        }
                        connection.sendTCP(tpresp);
                    }
                }
            });

    }
}

