package org.corfudb.runtime.protocols.replications;

import org.corfudb.runtime.*;
import org.corfudb.runtime.exceptions.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.smr.MultiCommand;

import javax.json.JsonObject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A replication protocol determines how writes are replicated across the configuration. We abstract away the protocol
 * so that safety and correctness can be isolated and proved entirely within the replication protocol.
 *
 * RP also determines how reads are performed so that safety is ensured. RP also determines where the hints are placed.
 * Hints should not affect safety at all (they are simply an optimization), but because the RP abstracts data placement,
 * it is necessary to read/write hints through the RP.
 *
 * RP interface unfortunately has significant overlap with the IWOAS interface, but because the two are abstracting
 * different ideas.
 *
 * Created by taia on 8/4/15.
 */
public interface IReplicationProtocol {

    static String getProtocolString()
    {
        return "unknown";
    }

    static Map<String, Object> segmentParser(JsonObject jo) {
        throw new UnsupportedOperationException("This replication protocol hasn't provided a way to parse config");
    }

    static IReplicationProtocol initProtocol(Map<String, Object> fields,
                                             Map<String,Class<? extends IServerProtocol>> availableLogUnitProtocols,
                                             Long epoch) {
        throw new UnsupportedOperationException("This replication protocol hasn't provided a way to parse config");
    }

    default List<IServerProtocol> getLoggingUnits() {
        throw new UnsupportedOperationException("This replication protocol hasn't provided getLoggingUnits");
    }

    default List<List<IServerProtocol>> getGroups() {
        throw new UnsupportedOperationException("This replication protocol hasn't provided getGroups");
    }

    default void write(CorfuDBRuntime client, long address, Set<UUID> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException {
        throw new UnsupportedOperationException("This replication protocol write hasn't been implemented");
    }

    default byte[] read(CorfuDBRuntime client, long address, UUID stream)
            throws UnwrittenException, TrimmedException {
        throw new UnsupportedOperationException("This replication protocol read hasn't been implemented");
    }

    default void readHints(long address) throws UnwrittenException, TrimmedException, NetworkException {
        throw new UnsupportedOperationException("This replication protocol readHints hasn't been implemented");
    }

    default void setHintsNext(long address, UUID stream, long nextOffset) throws UnwrittenException, TrimmedException, NetworkException {
        throw new UnsupportedOperationException("This address space doesn't support hints");
    }

    default void setHintsTxDec(long address, boolean dec) throws UnwrittenException, TrimmedException, NetworkException {
        throw new UnsupportedOperationException("This address space doesn't support hints");
    }

    default void setHintsFlatTxn(long address, MultiCommand flattedTxn) throws UnwrittenException, TrimmedException, IOException, NetworkException {
        throw new UnsupportedOperationException("This address space doesn't support hints");
    }
}
