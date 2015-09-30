package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.smr.MultiCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;

/**
 * Created by taia on 8/4/15.
 */
//public class QuorumReplicationProtocol implements IReplicationProtocol {
public class QuorumReplicationProtocol extends ChainReplicationProtocol {
    private static final Logger log = LoggerFactory.getLogger(QuorumReplicationProtocol.class);
    private ExecutorService executorService = Executors.newCachedThreadPool();



    public QuorumReplicationProtocol(List<List<IServerProtocol>> groups) {
        super(groups);
        log.info("new quorum protocol constructor");
    }

    public static String getProtocolString()
    {
        return "cdbqr";
    }

    public void quorumConnect(List<IServerProtocol> chain, long mappedAddress, Set<UUID> streams, byte[] data) {
        int sz = chain.size();
        AtomicInteger nsucceed = new AtomicInteger(0);
        AtomicInteger nfail = new AtomicInteger(0);
        Object quorumLock = new Object();
        Future[] chainFutures = new Future[sz]; int j = 0;

        for (IServerProtocol unit : chain) {
//            chainFutures[j++] = executorService.submit(() -> {
//                if (true) throw new IOException();
//                return null;
//            });
            chainFutures[j++] = executorService.submit(() -> {
                try {
                    ((IWriteOnceLogUnit) unit).write(mappedAddress, streams, data);
                    if (nsucceed.incrementAndGet() > sz / 2)
                        synchronized (quorumLock) {
                            quorumLock.notify();
                        }
                } catch (OutOfSpaceException e) {
                    if (nfail.incrementAndGet() >= sz / 2)
                        synchronized (quorumLock) {
                            quorumLock.notify();
                            throw e;
                        }
                } catch (TrimmedException e) {
                    nfail.set(sz);
                    synchronized (quorumLock) {
                        quorumLock.notify();
                        throw e;
                    }
                } catch (OverwriteException e) {
                    if (nfail.incrementAndGet() >= sz / 2) // TODO handle differently?
                        synchronized (quorumLock) {
                            quorumLock.notify();
                            throw e;
                        }
                } catch (NetworkException e) {
                    if (nfail.incrementAndGet() >= sz / 2)
                        synchronized (quorumLock) {
                            quorumLock.notify();
                            throw e;
                        }
                }

                return null;
            });

            try {
                synchronized (quorumLock) {
                    while (nsucceed.get() <= sz / 2 && !(nfail.get() >= sz / 2))
                        quorumLock.wait();
                }
                for (Future f : chainFutures) {
                    if (f.isDone()) f.get();
                }
            } catch (InterruptedException e) {
                // TODO this is a real error?
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void write(CorfuDBRuntime client, long address, Set<UUID> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException {
        // TODO: Handle multiple segments?

        while (true)
        {
            int mod = groups.size();
            int groupnum =(int) (address % mod);
            List<IServerProtocol> chain = groups.get(groupnum);
            long mappedAddress = address/mod;

            quorumConnect(chain, mappedAddress, streams, data);

            return;
        }

    }

    @Override
    public byte[] read(CorfuDBRuntime client, long address, UUID stream) throws UnwrittenException, TrimmedException {
        // TODO: Handle multiple segments?
        IReplicationProtocol reconfiguredRP = null;
        while (true)
        {
            try {
                if (reconfiguredRP != null) {
                    return reconfiguredRP.read(client, address, stream);
                }
                int mod = groups.size();
                int groupnum =(int) (address % mod);
                long mappedAddress = address/mod;

                List<IServerProtocol> chain = groups.get(groupnum);
                //reads have to come from last unit in chain
                IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
                return wolu.read(mappedAddress, stream);          }
            catch (NetworkException e)
            {
                log.warn("Unable to read, requesting new view.", e);
                client.invalidateViewAndWait(e);
                reconfiguredRP = client.getView().getSegments().get(0).getReplicationProtocol();
            }
        }
    }

}
