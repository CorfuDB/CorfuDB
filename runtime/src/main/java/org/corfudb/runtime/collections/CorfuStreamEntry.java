package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.Nonnull;

/**
 * Entry returned by CorfuStore's StreamListener interface.
 * NOTE: Ensure that the below protobuf generated classes are accessible to your JVM!
 * <p>
 * Created by hisundar on 2019-10-18
 *
 * @param <K> - type of the protobuf KeySchema defined while the table was created.
 * @param <V> - type of the protobuf PayloadSchema defined by the table creator.
 * @param <M> - type of the protobuf metadata schema defined by table creation.
 */
@Slf4j
public class CorfuStreamEntry<K extends Message, V extends Message, M extends Message> extends
        CorfuStoreEntry<K, V, M> {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CorfuStreamEntry<K, V, M> that = (CorfuStreamEntry<K, V, M>) o;

        return getOperation() == that.getOperation();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getOperation().hashCode();
        return result;
    }

    /**
     * Defines the type of the operation in this stream
     */
    public enum OperationType {
        UPDATE,
        DELETE,
        CLEAR
    }

    @Getter
    private final OperationType operation;

    public CorfuStreamEntry(K key, V payload, M metadata, OperationType operation) {
        super(key, payload, metadata);
        this.operation = operation;
    }

    /**
     * Convert a given SMREntry to CorfuStreamEntry.
     */
    public static <K extends Message, V extends Message, M extends Message>
    CorfuStreamEntry<K, V, M> fromSMREntry(SMREntry entry) {
        OperationType operationType = getOperationType(entry);
        // TODO(sneginhal): Need a way to differentiate between update and create.
        Object[] args = entry.getSMRArguments();

        K key = null;
        V payload = null;
        M metadata = null;
        if (args.length > 0) {
            key = (K) args[0];
            if (args.length > 1) {
                CorfuRecord record = (CorfuRecord) args[1];
                payload = (V) record.getPayload();
                metadata = (M) record.getMetadata();
            }
        }

        return new CorfuStreamEntry<>(key, payload, metadata, operationType);
    }

    private static OperationType getOperationType(@Nonnull SMREntry entry) {
        OperationType operationType;
        switch (entry.getSMRMethod()) {
            case "put":
            case "putAll":
                operationType = OperationType.UPDATE;
                break;
            case "clear":
                operationType = OperationType.CLEAR;
                break;
            case "remove":
                operationType = OperationType.DELETE;
                break;
            default:
                throw new RuntimeException("SMRMethod " + entry.getSMRMethod()
                        + " cannot be translated to any known operation type");
        }

        return operationType;
    }
}
