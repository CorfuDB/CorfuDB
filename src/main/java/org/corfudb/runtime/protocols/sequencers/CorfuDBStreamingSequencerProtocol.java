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

package org.corfudb.runtime.protocols.sequencers;

import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.PooledThriftClient;
import org.corfudb.runtime.NetworkException;
import org.corfudb.infrastructure.thrift.StreamingSequencerService;
import org.corfudb.infrastructure.thrift.StreamSequence;

import org.apache.thrift.protocol.TProtocol;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CorfuDBStreamingSequencerProtocol implements IServerProtocol, ISimpleSequencer, IStreamSequencer
{
    private String host;
    private Integer port;
    private Map<String,String> options;

    private final PooledThriftClient<StreamingSequencerService.Client, StreamingSequencerService.AsyncClient> thriftPool;
    private Logger log = LoggerFactory.getLogger(CorfuDBStreamingSequencerProtocol.class);


    public static String getProtocolString()
    {
        return "cdbsts";
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

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new CorfuDBStreamingSequencerProtocol(host, port, options);
    }

    private CorfuDBStreamingSequencerProtocol(String host, Integer port, Map<String,String> options)
    {
        this.host = host;
        this.port = port;
        this.options = options;

        try
        {
            thriftPool = new PooledThriftClient<StreamingSequencerService.Client, StreamingSequencerService.AsyncClient>(
                    new PooledThriftClient.ClientFactory<StreamingSequencerService.Client>() {
                        @Override
                        public StreamingSequencerService.Client make(TProtocol protocol)
                        {
                            return new StreamingSequencerService.Client(protocol);
                        }
                    },
                    StreamingSequencerService.AsyncClient::new,
                    new Config(),
                    host,
                    port
            );
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + getFullString());
            throw new RuntimeException("Failed to connect to endpoint");
        }
    }
    public long sequenceGetNext(UUID stream, int numTokens)
    throws NetworkException
    {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            StreamSequence ret = client.nextstreampos(stream.toString(), numTokens);
            thriftPool.returnResourceObject(client);
            return ret.position;
        }
        catch (Exception e)
        {
            log.warn("Exception getting next sequence", e);
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    public long sequenceGetCurrent(UUID stream)
    throws NetworkException
    {
       StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            StreamSequence ret = client.nextstreampos(stream.toString(), 0);
            thriftPool.returnResourceObject(client);
            return ret.position;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }


    public long sequenceGetNext()
    throws NetworkException
    {
         StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            long ret = client.nextpos(1);
            thriftPool.returnResourceObject(client);
            return ret;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    public long sequenceGetNext(int numTokens)
    throws NetworkException
    {
         StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            long ret = client.nextpos(numTokens);
            thriftPool.returnResourceObject(client);
            return ret;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    public long sequenceGetCurrent()
    throws NetworkException
    {
         StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            long ret = client.nextpos(0);
            thriftPool.returnResourceObject(client);
            return ret;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    @Override
    public void recover(long lastPos) throws NetworkException {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            client.recover(lastPos);
            thriftPool.returnResourceObject(client);
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    public boolean ping()
    {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            boolean ret = client.ping();
            thriftPool.returnResourceObject(client);
            return ret;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            return false;
        }
    }

    public void setEpoch(long epoch)
    {

    }

    public void reset(long epoch)
    throws NetworkException
    {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            client.reset();
            thriftPool.returnResourceObject(client);
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    public void setAllocationSize(UUID stream, int size)
    throws NetworkException
    {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            client.setAllocationSize(stream.toString(), size);
            thriftPool.returnResourceObject(client);
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            throw new NetworkException("Couldn't connect to endpoint!", this);
        }
    }

    /**
     * Simulates a failure by causing the node to not respond.
     * If not implemented, will throw an UnsupportedOperation exception.
     *
     * @param fail True, to simulate failure, False, to restore the unit to responsiveness.
     */
    @Override
    public void simulateFailure(boolean fail, long length) {
        StreamingSequencerService.Client client = null;
        try {
            client = thriftPool.getResource();
            client.simulateFailure(fail, length);
            thriftPool.returnResourceObject(client);
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
        }
    }

}


