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
        if (actual.getServerContext().getServerEpoch() != epoch) {
            failWithMessage("Expected server to be in epoch <%d> but it was in epoch <%d>", epoch,
                    actual.getServerContext().getServerEpoch());
        }
        return this;
    }

    public LayoutServerAssertions isPhase1Rank(Rank phase1Rank) {
        isNotNull();
        if (!actual.getPhase1Rank().equals(phase1Rank)) {
            failWithMessage("Expected server to be in phase1Rank <%s> but it was in phase1Rank <%s>", phase1Rank,
                    actual.getPhase1Rank());
        }
        return this;
    }

    public LayoutServerAssertions isPhase2Rank(Rank phase2Rank) {
        isNotNull();
        if (!actual.getPhase2Rank().equals(phase2Rank)) {
            failWithMessage("Expected server to be in phase2Rank <%s> but it was in phase2Rank <%s>", phase2Rank,
                    actual.getPhase2Rank());
        }
        return this;
    }

    public LayoutServerAssertions isProposedLayout(Layout layout) {
        isNotNull();
        if (!actual.getProposedLayout().asJSONString().equals(layout.asJSONString())) {
            failWithMessage("Expected server to have proposedLayout  <%s> but it is <%s>", layout,
                    actual.getProposedLayout());

        }
        return this;
    }
 }
