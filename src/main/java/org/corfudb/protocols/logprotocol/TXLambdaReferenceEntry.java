package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.ICorfuObject;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 4/3/16.
 */
@Slf4j
@ToString(callSuper = true)
@NoArgsConstructor
public class TXLambdaReferenceEntry extends LogEntry {

    public final static LambdaLock globalLock = new LambdaLock(new ReentrantLock());
    @Getter
    Method method;
    // May be null, if the method was static.
    @Getter
    ICorfuObject transactionalObject;
    @Getter
    Object[] lambdaArguments;
    @Getter
    Serializers.SerializerType serializerType;

    public TXLambdaReferenceEntry(Method lambdaReference, ICorfuObject transactionalObject,
                                  Object[] lambdaArguments, Serializers.SerializerType serializer) {
        super(LogEntryType.TX_LAMBDAREF);
        this.method = lambdaReference;
        this.lambdaArguments = lambdaArguments;
        this.serializerType = serializer;
        this.transactionalObject = transactionalObject;
    }

    public static LambdaLock getLockForTXAddress(long address) {
        return globalLock;
    }

    @SuppressWarnings("unchecked")
    public synchronized Object invoke() {
        try {
            method.setAccessible(true);
            Object ret = method.invoke(transactionalObject, lambdaArguments);
            if (runtime.getObjectsView().getTxFuturesMap().containsKey(entry.getGlobalAddress())) {
                runtime.getObjectsView().getTxFuturesMap().get(entry.getGlobalAddress()).complete(ret);
                runtime.getObjectsView().getTxFuturesMap().remove(entry.getGlobalAddress());
            }
            return ret;
        } catch (IllegalAccessException | InvocationTargetException nsme) {
            runtime.getObjectsView().getTxFuturesMap().get(entry.getGlobalAddress()).completeExceptionally(nsme);
            runtime.getObjectsView().getTxFuturesMap().remove(entry.getGlobalAddress());
            throw new RuntimeException(nsme);
        }
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param b The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(method.toString().length());
        b.writeBytes(method.toString().getBytes());
        b.writeBoolean(transactionalObject == null);
        if (transactionalObject != null) {
            UUID streamID = transactionalObject.getStreamID();
            b.writeLong(streamID.getMostSignificantBits());
            b.writeLong(streamID.getLeastSignificantBits());
        }
        b.writeByte(serializerType.asByte());
        b.writeByte(lambdaArguments.length);
        Arrays.stream(lambdaArguments)
                .forEach(x -> {
                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    Serializers.getSerializer(serializerType).serialize(x, b);
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param b The buffer to serialize from.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);

        // Now we have to find the Method....
        String methodName = new String(methodBytes);
        if (!b.readBoolean()) {
            UUID streamID = new UUID(b.readLong(), b.readLong());
            Class<ICorfuObject> c = ReflectionUtils.getClassFromMethodToString(methodName);
            transactionalObject = rt.getObjectsView().build()
                    .setStreamID(streamID)
                    .setType(c)
                    .open();
        }

        method = ReflectionUtils.getMethodFromToString(methodName);
        serializerType = Serializers.typeMap.get(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = Serializers.getSerializer(serializerType).deserialize(objBuf, rt);
            b.skipBytes(len);
        }
        lambdaArguments = arguments;
    }

    @Getter
    @Data
    public static class LambdaLock implements AutoCloseable {

        final Lock lock;

        public void close() {
            lock.unlock();
        }
    }
}
