package org.corfudb.generator.verification;

import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.KeysState.VersionedKey;
import org.corfudb.generator.state.State;

import java.util.Optional;

public class ReadOperationVerification implements Verification {

    private final State state;
    private final Operation.Context context;

    public ReadOperationVerification(State state, Operation.Context context) {
        this.state = state;
        this.context = context;
    }

    @Override
    public boolean verify() {
        FullyQualifiedKey fqKey = context.getFqKey();
        VersionedKey keyState = state.getKey(fqKey);

        Optional<String> stateValue = keyState.get(context.getVersion()).getValue();
        return stateValue.equals(context.getVal());
    }
}
