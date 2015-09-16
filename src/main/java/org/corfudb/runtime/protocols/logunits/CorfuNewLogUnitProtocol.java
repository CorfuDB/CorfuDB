/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime.protocols.logunits;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.thrift.TException;
import org.corfudb.infrastructure.thrift.*;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.PooledThriftClient;
import org.corfudb.util.Utils;

import java.awt.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class CorfuNewLogUnitProtocol implements IServerProtocol, INewWriteOnceLogUnit
{
    private String host;
    private Integer port;
    private Map<String,String> options;
    public Long epoch;

    private final transient PooledThriftClient<NewLogUnitService.Client, NewLogUnitService.AsyncClient> thriftPool;

    public static String getProtocolString()
    {
        return "cnlu";
    }

    public Integer getPort()
    {
        return port;
    }

    public String getHost()
    {
        return host;
    }

    public Map<String,String> getOptions()
    {
        return options;
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        try (val t = thriftPool.getCloseableResource())
        {
            return t.getClient().ping();
        }
        catch (TException e)
        {
            return false;
        }
    }

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     *
     * @param epoch
     */
    @Override
    public void setEpoch(long epoch) {
        log.info("Epoch set to {}", epoch);
        this.epoch = epoch;
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {
        try (val t = thriftPool.getCloseableResource())
        {
            t.getClient().reset();
        }
        catch (TException e)
        {
            log.warn("Error resetting log unit!", e);
            throw new NetworkException("Couldn't reset logging unit", this);
        }
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new CorfuNewLogUnitProtocol(host, port, options, epoch);
    }

    public CorfuNewLogUnitProtocol(String host, Integer port, Map<String, String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;

        try
        {
            thriftPool = new PooledThriftClient<>(NewLogUnitService.Client::new,
                    NewLogUnitService.AsyncClient::new,
                    new Config(),
                    host,
                    port);
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + getFullString());
            throw new RuntimeException("Failed to connect to endpoint");
        }
    }


    @Override
    public CompletableFuture<Void> write(long address, Set<UUID> streams, ByteBuffer payload) throws OverwriteException, TrimmedException, NetworkException, OutOfSpaceException {
        CompletableFuture<Void> cf = new CompletableFuture<>();
            try (val t = thriftPool.getCloseableAsyncResource()) {
            /* convert streams to something usable by thrift */
                Set<org.corfudb.infrastructure.thrift.UUID> thriftUUIDS =
                        streams.stream()
                                .map(s -> new org.corfudb.infrastructure.thrift.UUID(s.getMostSignificantBits(), s.getLeastSignificantBits()))
                                .collect(Collectors.toSet());
                t.getAsyncClient().write(epoch, address, thriftUUIDS, payload, t.getCallback((NewLogUnitService.AsyncClient.write_call wc) -> {
                    try {
                        WriteResult wr = wc.getResult();
                        if (wr.getCode() == ErrorCode.ERR_OVERWRITE) {
                            if (!wr.isSetData() || wr.getData() == null) {
                                cf.completeExceptionally(new OverwriteException("Address was written to!", address, null));
                            } else {
                                cf.completeExceptionally(new OverwriteException("Address was written to!", address, ByteBuffer.wrap(wr.getData())));
                            }
                        } else {
                            cf.complete(null);
                        }
                    }
                    catch (TException te)
                    {
                        cf.completeExceptionally(te);
                    }
                }));
            }
            catch (TException t)
            {
                cf.completeExceptionally(t);
            }
        return cf;
    }

    @Override
    public CompletableFuture<WriteOnceLogUnitRead> read(long address) throws NetworkException {
        CompletableFuture<WriteOnceLogUnitRead> cf = new CompletableFuture<>();
        try (val t = thriftPool.getCloseableAsyncResource())
        {
            t.getAsyncClient().read(
                    epoch, address, t.getCallback((NewLogUnitService.AsyncClient.read_call rc) -> {
                        try {
                            ReadResult r = rc.getResult();
                            Set<UUID> streams = r.getStream() == null ? Collections.emptySet() : r.getStream().stream()
                                    .map(s -> new UUID(s.getMsb(), s.getLsb()))
                                    .collect(Collectors.toSet());
                            cf.complete(new WriteOnceLogUnitRead(r.getCode(), streams, r.data, r.getHints()));
                        }
                        catch (TException te)
                        {
                            cf.completeExceptionally(te);
                        }
                    }));
        }
        catch (TException e)
        {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    /** Trims the given stream, freeing up all entries up to and including the address given.
     *
     * @param stream                The stream to trim.
     * @param address               The address to trim.
     * @throws NetworkException     If there was an exception sending the trim request (which shouldn't happen
     *                              since this is a oneway communication.
     */
    @Override
    public void trim(UUID stream, long address) throws NetworkException {
        try (val t = thriftPool.getCloseableAsyncResource())
        {
            t.getAsyncClient().trim(epoch, Utils.toThriftUUID(stream), address, t.getCallback());
        }
        catch (Exception e)
        {
            log.info("exception", e);
        }
    }

    @Override
    public void fillHole(long address) {
        try (val t = thriftPool.getCloseableAsyncResource())
        {
            t.getAsyncClient().fillHole(address, t.getCallback());
        }
        catch (Exception e)
        {
            log.info("exception", e);
        }
    }

    @Override
    public void forceGC() {
        try (val t = thriftPool.getCloseableAsyncResource())
        {
            t.getAsyncClient().forceGC(t.getCallback());
        }
        catch (Exception e)
        {
            log.info("exception", e);
        }
    }

    @Override
    public void setGCInterval(long millis) {
        try (val t = thriftPool.getCloseableAsyncResource())
        {
            t.getAsyncClient().setGCInterval(millis, t.getCallback());
        }
        catch (Exception e)
        {
            log.info("exception", e);
        }
    }
}


