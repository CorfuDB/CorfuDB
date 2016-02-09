package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.AbstractReplicationView.ReadResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class StreamView implements AutoCloseable {

    CorfuRuntime runtime;

    /** The ID of the stream. */
    @Getter
    final UUID streamID;

    /** A pointer to the log. */
    final AtomicLong logPointer;

    public StreamView(CorfuRuntime runtime, UUID streamID)
    {
        this.runtime = runtime;
        this.streamID = streamID;
        this.logPointer = new AtomicLong(0);
    }

    /** Write an object to this stream, returning the physical address it
     * was written at.
     *
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object    The object to write to the stream.
     * @return          The address this
     */
    public long write(Object object)
    {
        return acquireAndWrite(object, null, null);
    }

    /** Write an object to this stream, returning the physical address it
     * was written at.
     *
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object              The object to write to the stream.
     * @param acquisitionCallback A function which will be called after the successful acquisition
     *                            of a token, but before the data is written.
     * @param deacquisitionCallback A function which will be called after an overwrite error is encountered
     *                              on a previously acquired token.
     * @return                    The address this object was written at.
     */
    public long acquireAndWrite(Object object, Consumer<Long> acquisitionCallback, Consumer<Long> deacquisitionCallback)
    {
        while (true) {
            SequencerClient.TokenResponse tokenResponse =
                    runtime.getSequencerView().nextToken(Collections.singleton(streamID), 1);
            long token = tokenResponse.getToken();
            log.trace("Write[{}]: acquired token = {}", streamID, token);
            if (acquisitionCallback != null) {
                acquisitionCallback.accept(token);
            }
            try {
                runtime.getAddressSpaceView().write(token, Collections.singleton(streamID),
                        object, tokenResponse.getBackpointerMap());
                return token;
            } catch (OverwriteException oe)
            {
                log.debug("Overwrite occurred at {}, retrying.", token);
            }
        }
    }

    /** Read the next item from the stream.
     * This method is synchronized to protect against multiple readers.
     *
     * @return          The next item from the stream.
     */
    @SuppressWarnings("unchecked")
    public synchronized ReadResult read()
    {
        while (true)
        {
            long thisRead = logPointer.getAndIncrement();
            log.trace("Read[{}]: reading at {}", streamID, thisRead);
            ReadResult r = runtime.getAddressSpaceView().read(thisRead);
            if (r.getResult().getResultType() == LogUnitReadResponseMsg.ReadResultType.EMPTY)
            {
                //determine whether or not this is a hole
                long latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
                log.trace("Read[{}]: latest token at {}", streamID, latestToken);
                if (latestToken < thisRead)
                {
                    logPointer.decrementAndGet();
                    return null;
                }
                log.debug("Read[{}]: hole detected at {} (token at {}), attempting fill.", streamID, thisRead, latestToken);
                try {
                    runtime.getAddressSpaceView().fillHole(thisRead);
                } catch (OverwriteException oe) {
                    //ignore overwrite.
                }
                r = runtime.getAddressSpaceView().read(thisRead);
                log.debug("Read[{}]: holeFill {} result: {}", streamID, thisRead, r.getResult().getResultType());
            }
            Set<UUID> streams = (Set<UUID>) r.getResult().getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM);
            if (streams != null && streams.contains(streamID))
            {
                log.trace("Read[{}]: valid entry at {}", streamID, thisRead);
                return r;
            }
        }
    }

    public synchronized ReadResult[] readTo(long pos) {
        long latestToken = pos;
        if (pos == Long.MAX_VALUE) {
            latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
            log.trace("Linearization point set to {}", latestToken);
        }
        ArrayList<ReadResult> al = new ArrayList<ReadResult>();
        while (logPointer.get() <= latestToken)
        {
            ReadResult r = read();
            if (r == null) {
                log.warn("ReadTo[{}]: Read returned null when it should not have!", streamID);
                throw new RuntimeException("Unexpected stream state, aborting.");
            }
            else {
                al.add(r);
            }
        }
        return al.toArray(new ReadResult[al.size()]);
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on object managed by the
     * {@code try}-with-resources statement.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}
