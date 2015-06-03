package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An interface to an SMR engine
 * Created by mwei on 5/1/15.
 */
public interface ISMREngine<T> {

    interface ISMREngineOptions
    {
        CompletableFuture<Object> getReturnResult();
        CorfuDBRuntime getRuntime();
        UUID getEngineID();
    }

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     * @return          The object maintained by the SMR engine.
     */
    T getObject();

    /**
     * Set the underlying object. This method should ONLY be used by a TX engine to
     * restore state.
     */
    void setObject(T object);

    /**
     * Synchronize the SMR engine to a given timestamp, or pass null to synchronize
     * the SMR engine as far as possible.
     * @param ts        The timestamp to synchronize to, or null, to synchronize to the most
     *                  recent version.
     */
    void sync(ITimestamp ts);

    /**
     * Propose a new command to the SMR engine.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     *
     * @param completion    A completable future which will be fulfilled once the command is proposed,
     *                      which is to be completed by the command.
     *
     * @param readOnly      Whether or not the command is read only.
     *
     * @return              A timestamp representing the timestamp that the command was proposed to.
     */
    ITimestamp propose(ISMREngineCommand<T> command, CompletableFuture<Object> completion, boolean readOnly);

    /**
     * Propose a new command to the SMR engine.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     *
     * @param completion    A completable future which will be fulfilled once the command is proposed,
     *                      which is to be completed by the command.
     *
     * @return              A timestamp representing the timestamp that the command was proposed to.
     */
    default ITimestamp propose(ISMREngineCommand<T> command, CompletableFuture<Object> completion)
    {
        return propose(command, completion, false);
    }

    /**
     * Propose a new command to the SMR engine. This convenience function allows you to pass
     * the command without the completable future.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     * @return              A timestamp representing the timestamp the command was proposed to.
     */
    default ITimestamp propose(ISMREngineCommand<T> command)
    {
        return propose(command, null, false);
    }

    /**
     * Propose a local command to the SMR engine. A local command is one which is executed locally
     * only, but may propose other commands which affect multiple objects.
     * @param command       A lambda representing the command to be proposed
     * @param completion    A completion to be fulfilled.
     * @param readOnly      True, if the command is read only, false otherwise.
     * @return              A timestamp representing the command proposal time.
     */
    default ITimestamp propose(ISMRLocalCommand<T> command, CompletableFuture<Object> completion, boolean readOnly)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Propose a local command to the SMR engine. A local command is one which is executed locally
     * only, but may propose other commands which affect multiple objects.
     * @param command       A lambda representing the command to be proposed.
     * @param completion    A completion to be fulfilled.
     * @return              A timestamp representing the command proposal time.
     */
    default ITimestamp propose(ISMRLocalCommand<T> command, CompletableFuture<Object> completion)
    {
        return propose(command, completion, false);
    }

    /**
     * Checkpoint the current state of the SMR engine.
     * @return              The timestamp the checkpoint was inserted at.
     */
    ITimestamp checkpoint()
        throws IOException;

    /**
     * Get the timestamp of the most recently proposed command.
     * @return              A timestamp representing the most recently proposed command.
     */
    ITimestamp getLastProposal();

    /**
     * Pass through to check for the underlying stream.
     * @return              A timestamp representing the most recently proposed command on a stream.
     */
    ITimestamp check();

    /**
     * Get the underlying stream ID.
     * @return              A UUID representing the ID for the underlying stream.
     */
    UUID getStreamID();

    /**
     * Get the CorfuDB instance that supports this SMR engine.
     * @return              A CorfuDB instance.
     */
    default ICorfuDBInstance getInstance() {
        throw new UnsupportedOperationException("not yet implemented...");
    }

    /**
     * find the appropriate constructor for the underlying object,
     * given the ctor args supplied in initArgs. This is something every
     * SMREngine implementation needs to be able to do (first pass implementation
     * appeared to assume that underlying objects always have a default ctor,
     * which is untenable.)
     * @param c -- class of underlying object
     * @param initArgs -- ctor args for the desired constructor
     * @param <T> --
     * @return the constructor matching the given argument list if one exists
     */
    default <T> Constructor<T> findConstructor(Class<T> c, Object[] initArgs) {
        if (initArgs == null)
            initArgs = new Object[0];
        for (Constructor con : c.getDeclaredConstructors()) {
            Class[] types = con.getParameterTypes();
            if (types.length != initArgs.length)
                continue;
            boolean match = true;
            for (int i = 0; i < types.length; i++) {
                Class need = types[i], got = initArgs[i].getClass();
                if (!need.isAssignableFrom(got)) {
                    if (need.isPrimitive()) {
                        match = (int.class.equals(need) && Integer.class.equals(got))
                                || (long.class.equals(need) && Long.class.equals(got))
                                || (char.class.equals(need) && Character.class.equals(got))
                                || (short.class.equals(need) && Short.class.equals(got))
                                || (boolean.class.equals(need) && Boolean.class.equals(got))
                                || (byte.class.equals(need) && Byte.class.equals(got));
                    } else {
                        match = false;
                    }
                }
                if (!match)
                    break;
            }
            if (match)
                return con;
        }
        throw new IllegalArgumentException("Cannot find an appropriate constructor for class " + c + " and arguments " + Arrays.toString(initArgs));
    }

}