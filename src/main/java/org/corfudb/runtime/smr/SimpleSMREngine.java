package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 5/1/15.
 */
public class SimpleSMREngine<T> implements ISMREngine<T> {

    IStream stream;
    T underlyingObject;
    ITimestamp streamPointer;

    public SimpleSMREngine(IStream stream, Class<T> type)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        this.stream = stream;
        streamPointer = stream.getCurrentPosition();
        underlyingObject = type.getConstructor().newInstance();
    }

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     *
     * @return The object maintained by the SMR engine.
     */
    @Override
    public T getObject() {
        return underlyingObject;
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
        if (ts == null)
        {
            ts = stream.check();
            if (ts.compareTo(streamPointer) <= 0)
            {
                //we've already read to the most recent position, no need to keep reading.
                return;
            }
        }
        while (ts.compareTo(streamPointer) > 0)
        {
            try {
                IStreamEntry entry = stream.readNextEntry();
                ISMREngineCommand<T> function = (ISMREngineCommand<T>) entry.getPayload();
                function.accept(underlyingObject, null);
            } catch (Exception e)
            {
                //ignore entries we don't know what to do about.
            }
            streamPointer = stream.getCurrentPosition();
        }
    }

    /**
     * Propose a new command to the SMR engine.
     *
     * @param command A lambda (BiConsumer) representing the command to be proposed.
     *                The first argument of the lambda is the object the engine is acting on.
     *                The second argument of the lambda contains some TX that the engine
     *                The lambda must be serializable TODO:should figure out how to check serializability.
     */
    @Override
    public ITimestamp propose(ISMREngineCommand<T> command) {
        try {
            return stream.append(command);
        }
        catch (Exception e)
        {
            //well, propose is technically not reliable, so we can just silently drop
            //any exceptions.
            System.out.println(e.getMessage());
            return null;
        }
    }
}
