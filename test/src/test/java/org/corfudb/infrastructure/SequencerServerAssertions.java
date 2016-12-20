package org.corfudb.infrastructure;

import org.assertj.core.api.AbstractAssert;

/**
 * Created by mwei on 2/2/16.
 */
public class SequencerServerAssertions extends AbstractAssert<SequencerServerAssertions, SequencerServer> {

    public SequencerServerAssertions(SequencerServer actual) {
        super(actual, SequencerServerAssertions.class);
    }

    public static SequencerServerAssertions assertThat(SequencerServer actual) {
        return new SequencerServerAssertions(actual);
    }

    public SequencerServerAssertions tokenIsAt(long address) {
        isNotNull();

        if (actual.getGlobalLogTail().get() != address) {
            failWithMessage("Expected token to be at <%d> but got <%d>!", address,
                    actual.getGlobalLogTail().get());
        }

        return this;
    }

}
