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
import org.corfudb.runtime.protocols.NullCallback;
import org.corfudb.runtime.protocols.PooledThriftClient;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;
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
    public void write(long address, Set<UUID> streams, ByteBuffer payload) throws OverwriteException, TrimmedException, NetworkException, OutOfSpaceException {
        try (val t = thriftPool.getCloseableResource())
        {
            /* convert streams to something usable by thrift */
            Set<org.corfudb.infrastructure.thrift.UUID> thriftUUIDS =
                    streams.stream()
                            .map(s -> new org.corfudb.infrastructure.thrift.UUID(s.getMostSignificantBits(), s.getLeastSignificantBits()))
                            .collect(Collectors.toSet());
            WriteResult wr = t.getClient().write(epoch, address, thriftUUIDS, payload);
            if (wr.getCode() == ErrorCode.ERR_OVERWRITE)
            {
                if (!wr.isSetData() || wr.getData() == null)
                    throw new OverwriteException("Address was written to!", address, null);
                throw new OverwriteException("Address was written to!", address, ByteBuffer.wrap(wr.getData()));
            }
        }
        catch (TException e)
        {
            log.warn("Error writing to log unit!", e);
            throw new NetworkException("Couldn't write to logging unit", this, address, true);
        }
    }

    @Override
    public WriteOnceLogUnitRead read(long address) throws NetworkException {
        try (val t = thriftPool.getCloseableResource())
        {
            ReadResult r = t.getClient().read(epoch, address);
            Set<UUID> streams = r.getStream() == null ? Collections.emptySet() : r.getStream().stream()
                                .map(s -> new UUID(s.getMsb(), s.getLsb()))
                                .collect(Collectors.toSet());
            return new WriteOnceLogUnitRead(r.getCode(), streams, r.data, r.getHints());
        }
        catch (TException e)
        {
            log.warn("Error writing to log unit!", e);
            throw new NetworkException("Couldn't read from logging unit", this, address, true);
        }
    }

    @Override
    public void trim(long address) throws NetworkException {

    }

    @Override
    public void fillHole(long address) {
        try (val t = thriftPool.getCloseableResource())
        {
            t.getAsyncClient().fillHole(address, new NullCallback());
        }
        catch (Exception e)
        {
            log.info("exception", e);
        }
    }
}


