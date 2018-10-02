package org.corfudb.generator.replayer.replayOperations;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.replayer.Event;

/**
 * Provide helper methods for replayer operations.
 * Created by Sam Behnam on 2/15/18.
 */
@Slf4j
public class OperationUtils {

    public static final int OBJECT_CACHE_SIZE = 26;

    private static Object[] valueObjects = new Object[OBJECT_CACHE_SIZE];

    static {
        valueObjects[0] = null;
        int size = 1;
        for (int i = 1; i < OBJECT_CACHE_SIZE; i++) {
            valueObjects[i] = new byte[size];
            size *= 2;
        }
    }

    // Return an object close to the requested size on a logistic scale.
    // The largest size is 2^24 byte
    static Object getValueObjectForSize(String size) {
        int index = 0;
        int sizeInt = Integer.valueOf(size.toString());
        while (sizeInt > 0) {
            sizeInt >>= 1;
            index++;
        }

        if (index >= OBJECT_CACHE_SIZE) {
            throw new RuntimeException("Unexpected value size:" + size.toString());
        }

        return valueObjects[index];
    }

    public static Operation replayableOperationOf(@NonNull Event event, @NonNull Configuration configuration) {
        final Event.Type eventType = event.getEventType();

        switch (eventType) {
            case OPPUT:
                return new PutOperation(configuration);
            case OPCLEAR:
                return new ClearOperation(configuration);
            case OPCONTAINSKEY:
                return new ContainsKeyOperation(configuration);
            case OPCONTAINSVALUE:
                return  new ContainsValueOperation(configuration);
            case OPENTRYSET:
                return new EntrySetOperation(configuration);
            case OPGET:
                return new GetOperation(configuration);
            case OPBLINDPUT:
                return new BlindPutOperation(configuration);
            case OPSIZE:
                return new SizeOperation(configuration);
            case OPISEMPTY:
                return new IsEmptyOperation(configuration);
            case OPKEYSET:
                return new KeySetOperation(configuration);
            case OPREMOVE:
                return new RemoveOperation(configuration);
            case OPVLAUES:
                return new ValuesOperation(configuration);
            case OPSCANFILTER:
                return new ScanAndFilterOperation(configuration);
            case OPSCANFILTERENTRY:
                return new ScanAndFilterByEntryOperation(configuration);
            case TXBEGIN:
                // TODO add snapshot transaction
                if (event.getTxType().equals("OPTIMISTIC")) {
                    return new OptimisticTxOperation(configuration);
                } else if (event.getTxType().equals("WRITE_AFTER_WRITE")) {
                    return new WriteAfterWriteTxOperation(configuration);
                } else if (event.getTxType().equals("SNAPSHOT")) {
                    // Currently simulating SNAPSHOT with WRITE_AFTER_WRITE.
                    log.warn("Transaction:{} is a Snapshot Tx and currently not supported.",
                            event.getTxId());
                    return new WriteAfterWriteTxOperation(configuration);
                } else {
                    log.error("Unsupported transaction:{} is dropped.", event.getTxId());
                    return new NoOperation(configuration);
                }
            case TXCOMMIT:
                return new CommitTxOperation(configuration);
            case TXABORT:
                return new AbortTxOperation(configuration);
                // Not supported.
            case OPPUTALL:
            default:
                return new NoOperation(configuration);
        }
    }
}