package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

import org.corfudb.infrastructure.thrift.*;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@NoArgsConstructor
public class NewLogUnitServer implements ICorfuDBServer, NewLogUnitService.AsyncIface {

    TServer server;
    Boolean running;
    @Getter
    Thread thread;

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        running = true;
        while (running) {
            TNonblockingServerTransport serverTransport;
            NewLogUnitService.AsyncProcessor<NewLogUnitServer> processor;
            log.debug("New log unit entering service loop.");
            try {
                serverTransport = new TNonblockingServerSocket(port);
                processor =
                        new NewLogUnitService.AsyncProcessor<NewLogUnitServer>(this);
                server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
                        .processor(processor)
                        .protocolFactory(TCompactProtocol::new)
                        .inputTransportFactory(new TFastFramedTransport.Factory())
                        .outputTransportFactory(new TFastFramedTransport.Factory())
                );
                log.info("New log unit starting on port " + port);
                server.serve();
            } catch (TTransportException e) {
                log.warn("New log unit encountered exception, restarting.", e);
            }
        }
        log.info("New log unit server shutting down.");
    }

    @Data
    public class NewLogUnitHints {
            HintType type;
            byte[] hintData;
    }

    /**
     * The port that this instance is being served on.
     *
     * @return The current port that this instance is being serviced on.
     */
    @Getter
    short port;

    /**
     * The current epoch of this log unit.
     *
     * @return The current epoch this instance is in.
     */
    @Getter
    long currentEpoch;

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    Cache<Long, ByteBuffer> dataCache;

    /**
     * This cache services requests for hints.
     */
    Cache<Long, Set<NewLogUnitHints>> hintCache;

    /**
     * Returns an instance (main thread) of this server.
     * @param configuration     The configuration from the parsed Yaml file.
     * @return                  A runnable representing the main thread of this log unit.
     */
    @Override
    public ICorfuDBServer getInstance(Map<String, Object> configuration) {
        parseConfiguration(configuration);
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        if (server != null)
        {
            running = false;
            server.stop();
        }
    }

    /**
     * Parses the configuration for this server.
     * @param configuration    The configuration from the parsed Yaml file.
     * @throws RuntimeException     If the provided configuration is invalid.
     */
    void parseConfiguration(Map<String, Object> configuration) {
        if (configuration.get("port") == null)
        {
            log.error("Required key port is missing from configuration!");
            throw new RuntimeException("Invalid configuration provided!");
        }
        port = ((Integer)configuration.get("port")).shortValue();

        // Currently, only an in-memory configuration is supported.
        dataCache = Caffeine.newBuilder()
                .weakKeys()
                .build();

        // Hints are always in memory and never persisted.
        hintCache = Caffeine.newBuilder()
                .weakKeys()
                .build();
    }

    /**
     * Validate basic server parameters: namely, what epoch we are in and if the unit is in simulated failure mode.
     * If the validation fails, we throw a TException which will be passed on to the client.
     * @param epoch         What epoch the client thinks we should be in.
     * @throws TException   If the validation fails.
     */
    public void validate(long epoch)
            throws TException
    {
        if (epoch != currentEpoch)
        {
            throw new TException("Request from wrong epoch, got " + epoch + " expected " + currentEpoch);
        }
    }

    /**
     * Perform an asynchronous write. When AsyncMethodCallback is invoked, the write is guaranteed to be persisted
     * according to the persistence requirements in the configuration file.
     * @param epoch     What epoch the client thinks we should be in.
     * @param offset    The global offset (address) we are reading at.
     * @param stream    Which streams this log entry belongs to.
     * @param payload   The payload to store at the address.
     * @param resultHandler An asynchronous method callback which will be invoked when write is completed.
     * @throws TException   If there was an IO exception during the handling of this call.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void write(long epoch, long offset, Set<UUID> stream, ByteBuffer payload, AsyncMethodCallback resultHandler) throws TException {
        validate(epoch);

        ByteBuffer dataBuffer;
        // The size of the metadata is equal to the number of stream UUIDs plus a 16bit int.
        int metadataSize = (stream.size() * 16) + 2;

        // in-memory mode just uses a bytebuffer which is not memory mapped.
        dataBuffer = ByteBuffer.allocateDirect(payload.remaining() + metadataSize);

        //dump the metadata in the buffer.
        dataBuffer.putShort((short)stream.size());
        for (UUID streamid : stream)
        {
            dataBuffer.putLong(streamid.getMsb());
            dataBuffer.putLong(streamid.getLsb());
        }

        //dump the payload in the buffer.
        dataBuffer.put(payload);
        dataBuffer.flip();

        //put the buffer in the cache, if in memory (if it's not in-memory, we can rely on the cache retrieval).
        ByteBuffer newVal = dataCache.get(offset, (address) -> dataBuffer);

        if (newVal != dataBuffer)
        {
            resultHandler.onComplete(new WriteResult().setCode(ErrorCode.ERR_OVERWRITE).setData(newVal));
            return;
        }
        resultHandler.onComplete(new WriteResult().setCode(ErrorCode.OK));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void read(long epoch, long offset, AsyncMethodCallback resultHandler) throws TException {
        validate(epoch);

        //fetch the address from the cache.
        ByteBuffer data = dataCache.getIfPresent(offset).duplicate();
        if (data == null)
        {
            resultHandler.onComplete(new ReadResult(ReadCode.READ_EMPTY, Collections.emptySet(), ByteBuffer.allocate(0), Collections.emptySet()));
        }
        else
        {
            //rewind the buffer
            data.clear();
            //decode the metadata
            short numStreams = data.getShort();
            Set<UUID> cset = new HashSet();
            for (short i = 0; i < numStreams; i++)
            {
                UUID streamId = new UUID();
                streamId.setMsb(data.getLong());
                streamId.setLsb(data.getLong());
                cset.add(streamId);
            }
            //write the buffer
            resultHandler.onComplete(new ReadResult(ReadCode.READ_OK, cset, data.slice(), Collections.emptySet()));
        }
    }

    @Override
    public void trim(long epoch, UUID stream, long prefix, AsyncMethodCallback resultHandler) throws TException {
        validate(epoch);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void ping(AsyncMethodCallback resultHandler) throws TException {
        resultHandler.onComplete(true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reset(AsyncMethodCallback resultHandler) throws TException {
        log.info("Reset requested!");
        dataCache.invalidateAll();
        resultHandler.onComplete(null);
    }
}
