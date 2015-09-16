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

package org.corfudb.runtime.protocols;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.TServiceClient;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool.BasePoolableObjectFactory;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * This class implements a pooled Thrift client which other protocols may use.
 * Somewhat based on http://vincentdevillers.blogspot.com/2013/11/pooling-thrift-client.html
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */
@Slf4j
public class PooledThriftClient<T extends TServiceClient, U extends TAsyncClient> implements AutoCloseable
{
    private final GenericObjectPool<T> pool;
    private final GenericObjectPool<U> asyncPool;

    public PooledThriftClient(ClientFactory<T> factory, AsyncClientFactory<U> asyncFactory, Config config, String host, int port)
    {
        this(factory, asyncFactory, new BinaryOverSocketProtocolFactory(host, port), config, host, port);
    }

    @SuppressWarnings("rawtypes")
    @SneakyThrows
    public PooledThriftClient(ClientFactory<T> factory,
                              AsyncClientFactory<U> asyncFactory,
                              ProtocolFactory pfactory,
                              Config config,
                              String host,
                              int port)
    {
        this.pool = new GenericObjectPool<>(new ThriftClientFactory(factory, pfactory), config);
        this.asyncPool = new GenericObjectPool<>
                (new AsyncThriftClientFactory(asyncFactory,
                        new TCompactProtocol.Factory(),
                        new TAsyncClientManager(),
                        host, port
                        ),
                        config);
    }

    @SuppressWarnings("rawtypes")
    @RequiredArgsConstructor
    class AsyncThriftClientFactory extends BasePoolableObjectFactory<U> {
        private final AsyncClientFactory<U> clientFactory;
        private final TProtocolFactory protocolFactory;
        private final TAsyncClientManager manager;
        private final String host;
        private final int port;


        @Override
        public U makeObject() throws Exception {
            return clientFactory.make(protocolFactory, manager, new TNonblockingSocket(host, port));
        }

        @Override
        public void destroyObject(U obj) throws Exception {
        }
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

    @FunctionalInterface
    public interface ClientFactory<T> {
        T make(TProtocol protocol);
    }

    @FunctionalInterface
    public interface AsyncClientFactory<U> {
        U make(TProtocolFactory protocol, TAsyncClientManager manager, TNonblockingTransport transport);
    }

    @FunctionalInterface
    public interface ProtocolFactory<T> {
        TProtocol make();
    }

    @Data
    public class TPooledClient<X extends T, Y extends U> implements AutoCloseable
    {
        final X client;
        final Y asyncClient;
        final PooledThriftClient<T, U> pool;

        @Override
        public void close() {
            pool.returnResourceObject(client);
            pool.returnAsyncResourceObject(asyncClient);
        }
    }

    @Data
    public class pooledCompletion<N> implements AsyncMethodCallback<N>
    {

        final TPooledAsyncClient client;
        final Consumer<N> callback;

        @Override
        @SuppressWarnings("unchecked")
        public void onComplete(N t) {
            //apparently, we are not supposed to reuse async clients. So we should refactor this code.
            client.getPool().returnAsyncResourceObject(client.getAsyncClient());
            callback.accept(t);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onError(Exception e) {
            log.warn("Async method encountered exception!", e);
           // client.getPool().returnAsyncResourceObject(client.getAsyncClient());
        }
    }

    @Data
    public class TPooledAsyncClient<X extends T, Y extends U> implements AutoCloseable
    {
        final Y asyncClient;
        final PooledThriftClient<T, U> pool;

        public pooledCompletion getCallback()
        {
            return new pooledCompletion(this, (t) -> {});
        }

        public <Z> pooledCompletion<Z> getCallback(Consumer<Z> completionFunc)
        {
            return new pooledCompletion<>(this, completionFunc);
        }

        @Override
        public void close() {
        }
    }

    public TPooledAsyncClient<T,U> getCloseableAsyncResource()
    {
        return new TPooledAsyncClient<>(getAsyncResource(), this);
    }

    public TPooledClient<T,U> getCloseableResource()
    {
        return new TPooledClient<>(getResource(), getAsyncResource(), this);
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
                TTransport transport = new TFastFramedTransport(new TSocket(host,port));
                try {
                transport.open();
                } catch (Exception e)
                {
                    throw new RuntimeException("Could not generate protocol", e);
                }
                return new TCompactProtocol(transport);
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

    public U getAsyncResource() {
        try {
            return asyncPool.borrowObject();
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

    public void returnAsyncResourceObject(U resource)
    {
        try {
            asyncPool.returnObject(resource);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not return resource object", e);
        }
    }

    public void returnBrokenResource(T resource)
    {
        try {
            log.warn("Warning, broken resource returned!");
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
