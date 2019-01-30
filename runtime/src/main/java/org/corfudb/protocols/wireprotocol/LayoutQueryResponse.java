package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;

public class LayoutQueryResponse implements ICorfuPayload<LayoutQueryResponse> {

    @Getter
    final Optional<Phase2Data> currentEpochPhase2;

    @Getter
    final Optional<Phase2Data> lastEpochPhase2;

    @Getter
    final Optional<Layout> lastCommittedLayout;

    @Getter
    final long currentEpoch;

    public LayoutQueryResponse(long epoch, Phase2Data lastPhase2Data,
                               Phase2Data secondLastPhase2Data, Layout committedLayout) {
        this.currentEpoch = epoch;
        this.currentEpochPhase2 = Optional.ofNullable(lastPhase2Data);
        this.lastEpochPhase2 = Optional.ofNullable(secondLastPhase2Data);
        this.lastCommittedLayout = Optional.ofNullable(committedLayout);
    }

    public LayoutQueryResponse(ByteBuf buf) {
        boolean currentPhase2DataBool = buf.readBoolean();

        if (currentPhase2DataBool) {
            this.currentEpochPhase2 = Optional.of(Phase2Data.deserialize(buf));
        } else {
            this.currentEpochPhase2 = Optional.empty();
        }

        boolean lastEpochPhase2Bool = buf.readBoolean();

        if (lastEpochPhase2Bool) {
            this.lastEpochPhase2 = Optional.of(Phase2Data.deserialize(buf));
        } else {
            this.lastEpochPhase2 = Optional.empty();
        }

        boolean lastCommittedBool = buf.readBoolean();

        if (lastCommittedBool) {
            this.lastCommittedLayout = Optional.of(Layout.deserialize(buf));
        } else {
            this.lastCommittedLayout = Optional.empty();;
        }

        this.currentEpoch = buf.readLong();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeBoolean(currentEpochPhase2.isPresent());

        if (currentEpochPhase2.isPresent()) {
            currentEpochPhase2.get().serialize(buf);
        }

        buf.writeBoolean(lastEpochPhase2.isPresent());

        if (lastEpochPhase2.isPresent()) {
            lastEpochPhase2.get().serialize(buf);
        }

        buf.writeBoolean(lastCommittedLayout.isPresent());

        if (lastCommittedLayout.isPresent()) {
            lastCommittedLayout.get().serialize(buf);
        }

        buf.writeLong(currentEpoch);
    }
}
