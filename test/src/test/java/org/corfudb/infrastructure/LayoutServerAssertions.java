package org.corfudb.infrastructure;

import org.assertj.core.api.AbstractAssert;
import org.corfudb.runtime.view.Layout;

/**
 * Created by mwei on 1/7/16.
 */
public class LayoutServerAssertions extends AbstractAssert<LayoutServerAssertions, LayoutServer> {

    public LayoutServerAssertions(LayoutServer actual) {
        super(actual, LayoutServerAssertions.class);
    }

    public static LayoutServerAssertions assertThat(LayoutServer actual) {
        return new LayoutServerAssertions(actual);
    }

    public LayoutServerAssertions layoutHasSequencerCount(int count) {
        isNotNull();
        if (actual.getCurrentLayout().getSequencers().size() != count) {
            failWithMessage("Expected server to be have <%d> sequencers but it had <%d>", count,
                    actual.getCurrentLayout().getSequencers().size());
        }
        return this;
    }

    public LayoutServerAssertions isInEpoch(long epoch) {
        isNotNull();
        final long serverEpoch = actual.getServerContext().getServerEpoch();
        if (serverEpoch != epoch) {
            failWithMessage("Expected server to be in epoch <%d> but it was in epoch <%d>",
                    epoch, serverEpoch);
        }
        return this;
    }

    public LayoutServerAssertions isPhase1Rank(Rank phase1Rank) {
        isNotNull();
        final long serverEpoch = actual.getServerContext().getServerEpoch();
        if (!actual.getPhase1Rank(serverEpoch).equals(phase1Rank)) {
            failWithMessage("Expected server to be in phase1Rank <%s> but it was in phase1Rank <%s>", phase1Rank,
                    actual.getPhase1Rank(serverEpoch));
        }
        return this;
    }

    public LayoutServerAssertions isPhase2Rank(Rank phase2Rank) {
        isNotNull();
        final long serverEpoch = actual.getServerContext().getServerEpoch();
        if (!actual.getPhase2Rank(serverEpoch).equals(phase2Rank)) {
            failWithMessage("Expected server to be in phase2Rank <%s> but it was in phase2Rank <%s>", phase2Rank,
                    actual.getPhase2Rank(serverEpoch));
        }
        return this;
    }

    public LayoutServerAssertions isProposedLayout(Layout layout) {
        isNotNull();
        final long serverEpoch = actual.getServerContext().getServerEpoch();
        if (!actual.getProposedLayout(serverEpoch).asJSONString().equals(layout.asJSONString())) {
            failWithMessage("Expected server to have proposedLayout  <%s> but it is <%s>", layout,
                    actual.getProposedLayout(serverEpoch));

        }
        return this;
    }
 }
