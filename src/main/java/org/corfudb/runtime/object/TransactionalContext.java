package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.util.serializer.Serializers;

import java.util.*;

/**
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {

    private static final ThreadLocal<Deque<TransactionalContext>> threadStack = ThreadLocal.withInitial(
            LinkedList<TransactionalContext>::new);

    @Getter
    UUID transactionID;

    @SuppressWarnings("unchecked")
    class TransactionalObjectData<T> {

        CorfuSMRObjectProxy<T> proxy;
        Object smrObjectClone;
        long readTimestamp;
        List<SMREntry> bufferedWrites;

        public TransactionalObjectData(CorfuSMRObjectProxy<T> proxy)
        {
            this.proxy = proxy;
            this.bufferedWrites = new ArrayList<>();
            this.readTimestamp = Long.MIN_VALUE;
        }

        public T readObject() {
                readTimestamp = proxy.timestamp;
                return (T) (smrObjectClone == null ? proxy.smrObject : smrObjectClone);
        }

        public T writeObject() {
            if (smrObjectClone == null) {
                log.debug("Cloning SMR object {} due to transactional write.", proxy.sv.getStreamID());

                smrObjectClone =
                        (T) Serializers.getSerializer(proxy.serializer).clone(proxy.smrObject, proxy.runtime);
            }
            return (T) smrObjectClone;
        }

        public T readWriteObject() {
            readTimestamp = proxy.timestamp;
            if (smrObjectClone == null) {
                log.debug("Cloning SMR object {} due to transactional write.", proxy.sv.getStreamID());

                smrObjectClone =
                        (T) Serializers.getSerializer(proxy.serializer).clone(proxy.smrObject, proxy.runtime);
            }
            return (T) smrObjectClone;
        }
    }

    Map<CorfuSMRObjectProxy, TransactionalObjectData> objectMap;

    public TransactionalContext() {
        transactionID = UUID.randomUUID();
        objectMap = new HashMap<>();
    }

    /** Open an object for reading. The implementation will avoid creating a copy of the object
     * if it has not already been done.
     *
     * @param proxy     The SMR Object proxy to get an object for reading.
     * @param <T>       The type of object to get for reading.
     * @return          An object for reading.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectRead(CorfuSMRObjectProxy<T> proxy)
    {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
                .readObject();
    }

    /** Open an object for writing. For opacity, the implementation will create a clone of the
     * object.
     * @param proxy     The SMR Object proxy to get an object for writing.
     * @param <T>       The type of object to get for writing.
     * @return          An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectWrite(CorfuSMRObjectProxy<T> proxy)
    {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
                .writeObject();
    }

    /** Open an object for reading and writing. For opacity, the implementation will create a clone of the
     * object.
     * @param proxy     The SMR Object proxy to get an object for writing.
     * @param <T>       The type of object to get for writing.
     * @return          An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectReadWrite(CorfuSMRObjectProxy<T> proxy)
    {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
                .readWriteObject();
    }

    /** Buffer away an object update, adding it to the write set that will be generated
     * in the resulting TXEntry.
     *
     * @param proxy         The SMR Object proxy to buffer for.
     * @param SMRMethod     The method being called.
     * @param SMRArguments  The arguments to that method.
     * @param serializer    The serializer to use.
     * @param <T>           The type of the proxy.
     */
    @SuppressWarnings("unchecked")
    public <T> void bufferObjectUpdate(CorfuSMRObjectProxy<T> proxy, String SMRMethod,
                                   Object[] SMRArguments, Serializers.SerializerType serializer)
    {
        objectMap
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
                .bufferedWrites.add(new SMREntry(SMRMethod, SMRArguments, serializer));
    }

    /** Compute and write a TXEntry for this transaction to insert into the log.
     *
     * @return  A TXEntry which represents this transactional context.
     */
    public TXEntry getEntry()
    {
        Map<UUID, TXEntry.TXObjectEntry> entryMap = new HashMap<>();
        objectMap.entrySet().stream()
                .forEach(x -> entryMap.put(x.getKey().sv.getStreamID(),
                        new TXEntry.TXObjectEntry(x.getKey().timestamp, x.getValue().bufferedWrites)));
        return new TXEntry(entryMap);
    }

    /** Returns the transaction stack for the calling thread.
     *
     * @return      The transaction stack for the calling thread.
     */
    public static Deque<TransactionalContext> getTransactionStack()
    {
        return ((Deque<TransactionalContext>)threadStack.get());
    }

    /** Returns the current transactional context for the calling thread.
     *
     * @return      The current transactional context for the calling thread.
     */
    public static TransactionalContext getCurrentContext() {
        return getTransactionStack().peekFirst();
    }

    /** Returns whether or not the calling thread is in a transaction.
     *
     * @return      True, if the calling thread is in a transaction.
     *              False otherwise.
     */
    public static boolean isInTransaction() {
        return getTransactionStack().peekFirst() != null;
    }

    public static TransactionalContext newContext() {
        TransactionalContext context = new TransactionalContext();
        getTransactionStack().addFirst(context);
        return context;
    }

    public static TransactionalContext removeContext() {
        return getTransactionStack().pollFirst();
    }
}
