package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;

import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 5/6/15.
 */
public class TimeTravelSMREngine<T> extends SimpleSMREngine<T> {

    ITimestamp lockTS;

    public TimeTravelSMREngine(IStream stream, Class<T> type)
    {
        super(stream, type);
    }


    /**
     * Synchronize the SMR engine to a given timestamp, or pass null to synchronize
     * the SMR engine as far as possible.
     *
     * @param ts The timestamp to synchronize to, or null, to synchronize to the most
     *           recent version.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void sync(ITimestamp ts) {
        if (this.lockTS != null)
        {
            return;
        }

        super.sync(ts);
    }

    @SuppressWarnings("unchecked")
    public void travelAndLock(ITimestamp lockTS)
    {
        this.lockTS = lockTS;

        synchronized (this)
        {
            // Reverse operations on the object until we get
            // to the desired timestamp.

            if (streamPointer.compareTo(lockTS) == 0) return;
            if (streamPointer.compareTo(lockTS) < 0)
            {
                while (streamPointer.compareTo(lockTS) < 0)
                {
                    streamPointer = stream.getNextTimestamp(streamPointer);
                    try {
                            ISMREngineCommand o = (ISMREngineCommand) stream.readObject(streamPointer);
                            o.accept(underlyingObject, new SimpleSMREngineOptions(new CompletableFuture<Object>()));
                    } catch (Exception e)
                    {

                    }
                }
            }
            else {
                while (streamPointer.compareTo(lockTS) > 0) {
                    // Can we reverse this operation?
                    try {
                        Object o = stream.readObject(streamPointer);
                        if (o instanceof ReversibleSMREngineCommand) {
                            ReversibleSMREngineCommand c = (ReversibleSMREngineCommand) o;
                            c.reverse(underlyingObject, new SimpleSMREngineOptions(new CompletableFuture<Object>()));
                        }
                        //this operation is non reversible, so unfortunately we have to play from the beginning...
                        OneShotSMREngine<T> smrOS = new OneShotSMREngine<T>(stream.getRuntime().openStream(stream.getStreamID(),
                                SimpleStream.class), type, streamPointer);
                        smrOS.sync(streamPointer);
                        underlyingObject = smrOS.getObject();
                    } catch (Exception e) {

                    }
                    streamPointer = stream.getPreviousTimestamp(streamPointer);
                }
            }
        }
    }

    public void unlock(ITimestamp ts)
    {
        this.lockTS = null;
    }
}
