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

//from apache blur
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AllArgsConstructor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsyncPooledThriftClient {

    public static final Log LOG = LogFactory.getLog(AsyncPooledThriftClient.class);

    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 5;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 60000;

    private int _maxConnectionsPerHost;
    private long _timeout;
    private long _pollTime = 5;
    private Map<String, AtomicInteger> _numberOfConnections = new ConcurrentHashMap<String, AtomicInteger>();

    private Map<Connection, BlockingQueue<TAsyncClient>> _clientMap = new ConcurrentHashMap<Connection, BlockingQueue<TAsyncClient>>();
    private Map<String, Constructor<?>> _constructorCache = new ConcurrentHashMap<String, Constructor<?>>();
    private TProtocolFactory _protocolFactory;
    private TAsyncClientManager _clientManager;
    private Collection<TNonblockingTransport> _transports = new LinkedBlockingQueue<TNonblockingTransport>();
    private Field _transportField;
    private final Connection _connection;

    private Random random = new Random();

    public AsyncPooledThriftClient(final Connection connection) throws IOException {
        this(DEFAULT_MAX_CONNECTIONS_PER_HOST, DEFAULT_CONNECTION_TIMEOUT, connection);
    }

    public AsyncPooledThriftClient(int maxConnectionsPerHost, int connectionTimeout, final Connection connection) throws IOException {
        _clientManager = new TAsyncClientManager();
        _protocolFactory = new TBinaryProtocol.Factory();
        _maxConnectionsPerHost = maxConnectionsPerHost;
        _timeout = connectionTimeout;
        _connection = connection;
        try {
            _transportField = TAsyncClient.class.getDeclaredField("___transport");
            _transportField.setAccessible(true);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _clientManager.stop();
        for (TNonblockingTransport transport : _transports) {
            transport.close();
        }
    }


    @AllArgsConstructor
    public class AsyncPooledThriftClientWrapper<T extends TAsyncClient> implements AutoCloseable
    {
        final T client;
        final AsyncPooledThriftClient clientPool;

        @Override
        public void close() throws Exception {
            clientPool.returnClient(client);
        }
    }

    /**
     * Gets a client instance that implements the AsyncIface interface that
     * connects to the given connection string.
     *
     * @param <T>
     * @param asyncIfaceClass
     *          the AsyncIface interface to pool.
     * @param connectionStr
     *          the connection string.
     * @return the client instance.
     */
    @SuppressWarnings("unchecked")
    public <T> T getClient(final Class<T> asyncIfaceClass) {
        return (T) Proxy.newProxyInstance(asyncIfaceClass.getClassLoader(), new Class[] { asyncIfaceClass }, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return execute(new AsyncCall(asyncIfaceClass, method, args, _connection));
            }
        });
    }



    private Object execute(AsyncCall call) throws Exception {
        AsyncMethodCallback<?> realCallback = getRealAsyncMethodCallback(call._args);
        TAsyncClient client = newClient(call._clazz, call._connection);
        AsyncMethodCallback<?> retryingCallback = wrapCallback(realCallback, client, call._connection);
        resetArgs(call._args, retryingCallback);
        return call._method.invoke(client, call._args);
    }

    private synchronized BlockingQueue<TAsyncClient> getQueue(Connection connection) {
        BlockingQueue<TAsyncClient> blockingQueue = _clientMap.get(connection);
        if (blockingQueue == null) {
            blockingQueue = new LinkedBlockingQueue<TAsyncClient>();
            _clientMap.put(connection, blockingQueue);
        }
        return blockingQueue;
    }

    private void returnClient(TAsyncClient client) throws InterruptedException {
        if (!client.hasError()) {
            getQueue(_connection).put(client);
        } else {
            AtomicInteger counter = _numberOfConnections.get(_connection.getHost());
            if (counter != null) {
                counter.decrementAndGet();
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private AsyncMethodCallback<?> wrapCallback(AsyncMethodCallback<?> realCallback, TAsyncClient client, Connection connectionStr) {
        return new ClientPoolAsyncMethodCallback(realCallback, client, this, connectionStr);
    }

    private void resetArgs(Object[] args, AsyncMethodCallback<?> callback) {
        args[args.length - 1] = callback;
    }

    private AsyncMethodCallback<?> getRealAsyncMethodCallback(Object[] args) {
        return (AsyncMethodCallback<?>) args[args.length - 1];
    }

    private TAsyncClient newClient(Class<?> c, Connection connection) throws InterruptedException {
        BlockingQueue<TAsyncClient> blockingQueue = getQueue(connection);
        TAsyncClient client = blockingQueue.poll();
        if (client != null) {
            return client;
        }

        AtomicInteger counter;
        synchronized (_numberOfConnections) {
            counter = _numberOfConnections.get(connection.getHost());
            if (counter == null) {
                counter = new AtomicInteger();
                _numberOfConnections.put(connection.getHost(), counter);
            }
        }

        synchronized (counter) {
            int numOfConnections = counter.get();
            while (numOfConnections >= _maxConnectionsPerHost) {
                client = blockingQueue.poll(_pollTime, TimeUnit.MILLISECONDS);
                if (client != null) {
                    return client;
                }
                LOG.debug("Waiting for client number of connection [" + numOfConnections + "], max connection per host [" + _maxConnectionsPerHost + "]");
                numOfConnections = counter.get();
            }
            LOG.info("Creating a new client for [" + connection + "]");
            String name = c.getName();
            Constructor<?> constructor = _constructorCache.get(name);
            if (constructor == null) {
                String clientClassName = name.replace("$AsyncIface", "$AsyncClient");
                try {
                    Class<?> clazz = Class.forName(clientClassName);
                    constructor = clazz.getConstructor(new Class[] { TProtocolFactory.class, TAsyncClientManager.class, TNonblockingTransport.class });
                    _constructorCache.put(name, constructor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                TNonblockingSocket transport = newTransport(connection);
                client = (TAsyncClient) constructor.newInstance(new Object[] { _protocolFactory, _clientManager, transport });
                client.setTimeout(_timeout);
                counter.incrementAndGet();
                return client;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private TNonblockingSocket newTransport(Connection connection) throws IOException {
        return new TNonblockingSocket(connection.getHost(), connection.getPort());
    }

    private static class ClientPoolAsyncMethodCallback<T> implements AsyncMethodCallback<T> {

        private AsyncMethodCallback<T> _realCallback;
        private TAsyncClient _client;
        private AsyncPooledThriftClient _pool;
        private Connection _connection;

        public ClientPoolAsyncMethodCallback(AsyncMethodCallback<T> realCallback, TAsyncClient client, AsyncPooledThriftClient pool, Connection connection) {
            _realCallback = realCallback;
            _client = client;
            _pool = pool;
            _connection = connection;
        }

        @Override
        public void onComplete(T response) {
            _realCallback.onComplete(response);
            try {
                _pool.returnClient(_client);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception exception) {
            AtomicInteger counter = _pool._numberOfConnections.get(_connection.getHost());
            if (counter != null) {
                counter.decrementAndGet();
            }
            _realCallback.onError(exception);
            _pool.closeAndRemoveTransport(_client);
        }
    }

    private static class AsyncCall {

        Class<?> _clazz;
        Method _method;
        Object[] _args;
        Connection _connection;

        public AsyncCall(Class<?> clazz, Method method, Object[] args, Connection connection) {
            _clazz = clazz;
            _method = method;
            _args = args;
            _connection = connection;
        }
    }

    private void closeAndRemoveTransport(TAsyncClient client) {
        try {
            TNonblockingTransport transport = (TNonblockingTransport) _transportField.get(client);
            _transports.remove(transport);
            transport.close();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}