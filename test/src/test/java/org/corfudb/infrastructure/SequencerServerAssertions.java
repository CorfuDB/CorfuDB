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

}
