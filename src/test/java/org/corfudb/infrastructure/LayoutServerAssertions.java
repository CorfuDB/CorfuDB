package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.assertj.core.api.AbstractAssert;
import org.corfudb.protocols.wireprotocol.LogUnitPayloadMsg;

/**
 * Created by mwei on 1/7/16.
 */
public class LayoutServerAssertions extends AbstractAssert<LayoutServerAssertions, LayoutServer> {

    public LayoutServerAssertions(LayoutServer actual)
    {
        super(actual, LayoutServerAssertions.class);
    }

    public static LayoutServerAssertions assertThat(LayoutServer actual)
    {
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
        if (actual.getCurrentLayout().getEpoch() != epoch) {
            failWithMessage("Expected server to be in epoch <%d> but it was in epoch <%d>", epoch,
                    actual.getCurrentLayout().getEpoch());
        }
        return this;
    }

    public LayoutServerAssertions isPhase1Rank(long phase1Rank) {
        isNotNull();
        if (actual.getPhase1Rank().compareTo(phase1Rank) != 0) {
            failWithMessage("Expected server to be in phase1Rank <%d> but it was in phase1Rank <%d>", phase1Rank,
                    actual.getPhase1Rank());
        }
        return this;
    }

    public LayoutServerAssertions isPhase2Rank(long phase2Rank) {
        isNotNull();
        if (actual.getPhase2Rank().compareTo(phase2Rank) != 0) {
            failWithMessage("Expected server to be in phase2Rank <%d> but it was in phase2Rank <%d>", phase2Rank,
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
