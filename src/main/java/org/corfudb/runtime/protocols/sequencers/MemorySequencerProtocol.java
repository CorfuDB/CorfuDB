package org.corfudb.runtime.protocols.sequencers;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 4/30/15.
 */
public class MemorySequencerProtocol implements IStreamSequencer, ISimpleSequencer, IServerProtocol {

    private Logger log = LoggerFactory.getLogger(MemorySequencerProtocol.class);
    private Map<String,String> options;
    private String host;
    private Integer port;
    private AtomicLong sequenceNumber;
    private Long epoch;
    public static ConcurrentHashMap<Integer, MemorySequencerProtocol> memorySequencers =
            new ConcurrentHashMap<Integer, MemorySequencerProtocol>();
    public MemorySequencerProtocol() {
        this("localhost", 0, new HashMap<String,String>(), 0L);
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        IServerProtocol res;
        if ((res = memorySequencers.get(port)) != null)
        {
            return res;
        }
        return new MemorySequencerProtocol(host, port, options, epoch);
    }

    public MemorySequencerProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        sequenceNumber = new AtomicLong();
        log.info("Creating new memory sequencer on virtual port " + this.port);
        memorySequencers.put(this.port, this);
    }

    public static String getProtocolString()
    {
        return "ms";
    }

    /**
     * Returns the host
     *
     * @return The hostname for the server.
     */
    @Override
    public String getHost() {
        return host;
    }

    /**
     * Returns the port
     *
     * @return The port number of the server.
     */
    @Override
    public Integer getPort() {
        return port;
    }

    /**
     * Returns the option map
     *
     * @return The option map for the server.
     */
    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        return true;
    }

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     *
     * @param epoch
     */
    @Override
    public void setEpoch(long epoch) {
        this.epoch =epoch;
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {
        sequenceNumber = new AtomicLong();
        log.info("Sequencer reset, epoch@{}, sequence@{}", epoch, sequenceGetCurrent());
    }

    @Override
    public long sequenceGetNext() throws NetworkException {
        return sequenceNumber.getAndIncrement();
    }

    @Override
    public long sequenceGetNext(int numTokens) throws NetworkException {
        return sequenceNumber.getAndAccumulate(numTokens, (p,n) -> n+p);
    }

    @Override
    public long sequenceGetCurrent() throws NetworkException {
        return sequenceNumber.get();
    }

    @Override
    public void recover(long lastPos) throws NetworkException {
        sequenceNumber.set(lastPos);
    }

    @Override
    public long sequenceGetNext(UUID stream, int count) throws NetworkException {
        return sequenceNumber.getAndAccumulate(count, (p, n) -> n + p);
    }

    @Override
    public long sequenceGetCurrent(UUID stream) throws NetworkException {
        return sequenceNumber.get();
    }

    @Override
    public void setAllocationSize(UUID stream, int count) throws NetworkException {

    }
}
