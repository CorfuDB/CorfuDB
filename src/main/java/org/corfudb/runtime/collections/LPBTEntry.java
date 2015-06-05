package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by crossbach on 5/29/15.
 */
public class LPBTEntry<K extends Comparable<K>, V> implements ICorfuDBObject<LPBTEntry<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(LPBTEntry.class);

    transient ISMREngine<TreeEntry> smr;
    ITransaction tx;
    UUID streamID;

    public LPBTEntry(LPBTEntry<K, V> entry, ITransaction tx) {
        this.streamID = entry.streamID;
        this.tx = tx;
    }

    @SuppressWarnings("unchecked")
    public LPBTEntry(IStream stream, Class<? extends ISMREngine> smrClass) {
        try {
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, TreeEntry.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public LPBTEntry(IStream stream) {
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<TreeEntry>(stream, TreeEntry.class);
    }

    @Override
    public Class<?> getUnderlyingType() {
        return TreeEntry.class;
    }

    @Override
    public UUID getStreamID() {
        return streamID;
    }

    @Override
    public ISMREngine getUnderlyingSMREngine() {
        return smr;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setUnderlyingSMREngine(ISMREngine engine) {
        this.smr = engine;
    }

    /**
     * read the deleted flag on this entry
     *
     * @return
     */
    public Boolean readDeleted() {
        return (Boolean) accessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            opts.getReturnResult().complete(entry.deleted);
        });
    }

    /**
     * apply a delete command
     *
     * @param b
     * @return
     */
    public Boolean writeDeleted(boolean b) {
        return (Boolean) mutatorAccessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            entry.deleted = b;
            opts.getReturnResult().complete(b);
        });
    }

    /**
     * read the key
     *
     * @return
     */
    public K readKey() {
        return (K) accessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            opts.getReturnResult().complete(entry.key);
        });
    }

    /**
     * write the key
     *
     * @param k
     * @return
     */
    public K writeKey(K k) {
        return (K) mutatorAccessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            entry.key = k;
            CompletableFuture cf = opts.getReturnResult();
            log.warn("writeKey(" + k + "), cf=" + cf);
            cf.complete(k);
        });
    }

    /**
     * read the value
     *
     * @return
     */
    public V readValue() {
        return (V) accessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            opts.getReturnResult().complete(entry.value);
        });
    }

    /**
     * write the value
     *
     * @param v
     * @return
     */
    public V writeValue(V v) {
        return (V) mutatorAccessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            entry.value = v;
            opts.getReturnResult().complete(v);
        });
    }

    /**
     * read the next pointer
     *
     * @return
     */
    public UUID readNext() {
        return (UUID) accessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            opts.getReturnResult().complete(entry.oidnext);
        });
    }

    /**
     * write the next pointer
     *
     * @param _next
     * @return
     */
    public UUID writeNext(UUID _next) {
        return (UUID) mutatorAccessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            entry.oidnext = _next;
            opts.getReturnResult().complete(_next);
        });
    }

    /**
     * toString
     *
     * @return
     */
    @Override
    public String toString() {
        return (String) accessorHelper((ISMREngineCommand<TreeEntry>) (entry, opts) -> {
            StringBuilder sb = new StringBuilder();
            if (entry.deleted)
                sb.append("DEL: ");
            sb.append("E");
            sb.append(streamID);
            sb.append(":[k=");
            sb.append(entry.key);
            sb.append(", v=");
            sb.append(entry.value);
            sb.append(", n=");
            sb.append(entry.oidnext);
            sb.append("]");
            opts.getReturnResult().complete(sb.toString());
        });
    }
}
