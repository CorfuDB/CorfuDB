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

package org.corfudb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TServiceClient;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool.BasePoolableObjectFactory;

/**
 * This class implements a pooled Thrift client which other protocols may use.
 * Somewhat based on http://vincentdevillers.blogspot.com/2013/11/pooling-thrift-client.html
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */
public class PooledThriftClient<T extends TServiceClient> implements AutoCloseable
{
    private final static Logger log = LoggerFactory.getLogger(PooledThriftClient.class);

    private final GenericObjectPool<T> pool;

    public PooledThriftClient(ClientFactory<T> factory, Config config, String host, int port)
    {
        this(factory, new BinaryOverSocketProtocolFactory(host, port), config);
    }

    @SuppressWarnings("rawtypes")
    public PooledThriftClient(ClientFactory<T> factory, ProtocolFactory pfactory, Config config)
    {
        this.pool = new GenericObjectPool<T>(new ThriftClientFactory(factory, pfactory), config);
    }

    @SuppressWarnings("rawtypes")
    class ThriftClientFactory extends BasePoolableObjectFactory<T> {
        private ClientFactory<T> clientFactory;
        private ProtocolFactory protocolFactory;

        public ThriftClientFactory(ClientFactory<T> clientFactory, ProtocolFactory protocolFactory)
        {
            this.clientFactory = clientFactory;
            this.protocolFactory = protocolFactory;
        }

        @Override
        public T makeObject() throws Exception {
            TProtocol protocol = protocolFactory.make();
            return clientFactory.make(protocol);
        }

        @Override
        public void destroyObject(T obj) throws Exception {
            if (obj.getOutputProtocol().getTransport().isOpen()) {
                obj.getOutputProtocol().getTransport().close();
            }
            if (obj.getInputProtocol().getTransport().isOpen())
            {
                obj.getInputProtocol().getTransport().close();
            }
        }
    }

    public static interface ClientFactory<T> {
        T make(TProtocol protocol);
    }

    public static interface ProtocolFactory<T> {
        TProtocol make();
    }

    @SuppressWarnings("rawtypes")
    public static class BinaryOverSocketProtocolFactory implements ProtocolFactory {
            private String host;
            private int port;

            public BinaryOverSocketProtocolFactory(String host, int port)
            {
                this.host = host;
                this.port = port;
            }

            public TProtocol make() {
                TTransport transport = new TSocket(host,port);
                try {
                transport.open();
                } catch (Exception e)
                {
                    throw new RuntimeException("Could not generate protocol", e);
                }
                return new TBinaryProtocol(transport);
            }
    }

    public T getResource() {
        try {
            return pool.borrowObject();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not get resource", e);
        }
    }

    public void returnResourceObject(T resource)
    {
        try {
        pool.returnObject(resource);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not return resource object", e);
        }
    }

    public void returnBrokenResource(T resource)
    {
        try {
        pool.invalidateObject(resource);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not invalidate object", e);
        }
    }

    public void destroy() {
        close();
    }

    public void close() {
        try {
        pool.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not close pool", e);
        }
    }
}
