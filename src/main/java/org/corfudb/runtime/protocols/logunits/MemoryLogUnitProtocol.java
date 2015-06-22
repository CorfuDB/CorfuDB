package org.corfudb.runtime.protocols.logunits;

import org.apache.thrift.TException;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.SimpleLogUnitService;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mwei on 4/30/15.
 */
public class MemoryLogUnitProtocol implements IServerProtocol, IWriteOnceLogUnit {

    private Logger log = LoggerFactory.getLogger(MemoryLogUnitProtocol.class);
    private Map<String,String> options;
    private String host;
    private Integer port;
    private Long epoch;
    private Long trimMark;
    private boolean simFailure = false;

    private ConcurrentMap<Long, byte[]> memoryArray;
    private ConcurrentMap<Long, ExtntInfo> metadataMap;



    public static ConcurrentHashMap<Integer, MemoryLogUnitProtocol> memoryUnits =
            new ConcurrentHashMap<Integer, MemoryLogUnitProtocol>();

    public MemoryLogUnitProtocol() {
        this.host = "test";
        this.port = 9999;
        this.options = new HashMap<>();
        this.epoch = 0L;
        trimMark = 0L;
        memoryArray = new NonBlockingHashMapLong<byte[]>();
        metadataMap = new NonBlockingHashMapLong<ExtntInfo>();
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        IServerProtocol res;
        if ((res = memoryUnits.get(port)) != null)
        {
            return res;
        }
        return new MemoryLogUnitProtocol(host, port, options, epoch);
    }

    public MemoryLogUnitProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        trimMark = 0L;
        memoryArray = new NonBlockingHashMapLong<byte[]>();
        metadataMap = new NonBlockingHashMapLong<ExtntInfo>();
        memoryUnits.put(this.port, this);
    }

    public static String getProtocolString()
    {
        return "mlu";
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
        return !simFailure;
    }

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     *
     * @param epoch
     */
    @Override
    public void setEpoch(long epoch) {
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
        memoryArray = new NonBlockingHashMapLong<byte[]>();
        this.trimMark = 0L;
        this.epoch = epoch;
    }

    /**
     * Simulates a failure by causing the node to not respond.
     * If not implemented, will throw an UnsupportedOperation exception.
     *
     * @param fail True, to simulate failure, False, to restore the unit to responsiveness.
     */
    @Override
    public void simulateFailure(boolean fail) {
        simFailure = fail;
    }

    /**
     * Write to the stream unit.
     * @param address                   The address to write to
     * @param payload                   The payload to be written
     * @throws OverwriteException       If the address has already been written to.
     * @throws TrimmedException         If the address has been trimmed.
     * @throws NetworkException         If there is a network problem (not thrown by memory implementation).
     */
    @Override
    public void write(long address, byte[] payload) throws OverwriteException, TrimmedException, NetworkException, OutOfSpaceException {
        if (simFailure)
        {
            throw new NetworkException("Unit in simulated failure mode!", this, address, true);
        }
        if (address < trimMark)
        {
            throw new TrimmedException("Address is trimmed", address);
        }
        if (memoryArray.putIfAbsent(address, payload) != null)
        {
            throw new OverwriteException("Address already written to", address);
        }
    }

    /**
     * Read from the logunit.
     * @param address               The address to read from.
     * @return                      The data at that address.
     * @throws UnwrittenException   If there is no data at that address.
     * @throws TrimmedException     If the address has been trimmed.
     * @throws NetworkException     If there is a network problem (not thrown by memory implementation).
     */
    @Override
    public byte[] read(long address) throws UnwrittenException, TrimmedException, NetworkException {
        if (simFailure)
        {
            throw new NetworkException("Unit in simulated failure mode!", this, address, false);
        }
        if (address < trimMark)
        {
            throw new TrimmedException("Address is trimmed", address);
        }
        byte[] data = memoryArray.get(address);
        if (data == null)
        {
            throw new UnwrittenException("No data present at this address", address);
        }
        return data;
    }

    @Override
    public ExtntInfo readmeta(long address) throws TrimmedException, NetworkException
    {
        // TODO: Throw any exceptions?
        log.info("metadataMap is null? {}", metadataMap == null);
        return metadataMap.get(address);
    }

    @Override
    public void setmetaNext(long address, long nextOffset) throws TrimmedException, NetworkException {
        ExtntInfo extnt = metadataMap.get(address);
        if (extnt == null) {
            metadataMap.put(address, new ExtntInfo().setNextOff(nextOffset));
            return;
        }
        extnt.setNextOff(nextOffset);
    }

    @Override
    public void setmetaTxDec(long address, boolean dec) throws TrimmedException, NetworkException {
        ExtntInfo extnt = metadataMap.get(address);
        if (extnt == null) {
            metadataMap.put(address, new ExtntInfo().setTxDec(dec));
            return;
        }
        extnt.setTxDec(dec);
    }

    /**
     * Trim the logunit.
     * @param address               The address, exclusive, to prefix-trim to.
     *                              The resulting address space [0, address) will be trimmed.
     * @throws NetworkException
     */
    @Override
    public void trim(long address) throws NetworkException {
        trimMark = address;
        for (long i = 0; i < address; i++)
        {
            memoryArray.remove(address);
        }
    }
}
