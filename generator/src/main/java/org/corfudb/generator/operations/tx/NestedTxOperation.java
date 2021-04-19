package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.KeysState.ThreadName;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends AbstractTxOperation {

    private static final int MAX_NEST = 20;

    private final Map<Integer, List<Operation>> nestedOperations = new HashMap<>();
    private final int numNested;

    public NestedTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager,
                             Correctness correctness) {
        super(state, Operation.Type.TX_NESTED, operations, tablesManager, correctness);

        numNested = state.getOperationCount().sample();
        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            nestedOperations.put(i, createOperations());
        }
    }

    @Override
    public void execute() {
        correctness.recordTransactionMarkers(opType, TxStatus.START);
        startOptimisticTx();

        addToHistoryStartTx();

        int nestedTxToStop = numNested;

        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            try {
                startOptimisticTx();

                List<Operation> operationsToExecute = nestedOperations.get(i);
                for (Operation nestedOperation : operationsToExecute) {
                    nestedOperation.execute();
                }

            } catch (TransactionAbortedException tae) {
                log.warn("Transaction Aborted", tae);
                nestedTxToStop--;
            }
        }

        for (int i = 0; i < nestedTxToStop; i++) {
            stopTx();
        }
        long timestamp;
        try {
            timestamp = stopTx();
            Keys.Version currentVersion = Keys.Version.build(timestamp);
            correctness.recordTransactionMarkers(opType, TxStatus.END, currentVersion);
            addToHistoryStopTx(currentVersion);
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxStatus.ABORTED);
        }
    }

    private void addToHistoryStopTx(Keys.Version currentVersion) {
        TxState.TxContext txState = state.getTransactions().get(ThreadName.buildFromCurrentThread());
        txState.getSnapshotId().setVersion(currentVersion);
        txState.setTerminated(true);
    }

    private void addToHistoryStartTx() {
        KeysState.SnapshotId snapshotId = KeysState.SnapshotId.builder()
                .threadId(ThreadName.buildFromCurrentThread())
                .version(Keys.Version.noVersion())
                .clientId("client")
                .build();

        TxState.TxContext txContext = TxState.TxContext.builder()
                .opType(Type.TX_NESTED)
                .snapshotId(snapshotId)
                .startTxVersion(Keys.Version.noVersion())
                .build();
        state.getTransactions().put(ThreadName.buildFromCurrentThread(), txContext);
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("NestedTx doesn't have operation context");
    }

    @Override
    public List<Operation> getNestedOperations() {
        List<Operation> result = new ArrayList<>();

        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            result.addAll(nestedOperations.get(i));
        }

        return result;
    }
}
