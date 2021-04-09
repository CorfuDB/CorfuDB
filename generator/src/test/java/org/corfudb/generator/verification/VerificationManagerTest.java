package org.corfudb.generator.verification;

import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.state.State;
import org.junit.jupiter.api.Test;

class VerificationTest {

    @Test
    public void testReadOperationAndVerification() {
        State state = new State(1, 1, );
        ReadOperation read = new ReadOperation();
        ReadOperationVerification verification = new ReadOperationVerification();

        verification.verify();
    }

}