package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zlokhandwala on 4/7/17.
 */
@Data
@AllArgsConstructor
public class RouterRuleMsg implements ICorfuPayload<RouterRuleMsg> {

    private final Boolean clearAllRules;
    private final Set<CorfuMsgType> corfuMsgTypeSet;
    private final Double dropProbability;

    public RouterRuleMsg(boolean clearAllRules, List<String> corfuMsgTypeList, Double dropProbability) {
        this.clearAllRules = clearAllRules;
        this.corfuMsgTypeSet = corfuMsgTypeList.stream()
                .map(CorfuMsgType::valueOf)
                .collect(Collectors.toSet());
        this.dropProbability = dropProbability;
    }

    public RouterRuleMsg() {
        this(true, new HashSet<>(), 0.0);
    }

    public RouterRuleMsg(ByteBuf buf) {
        corfuMsgTypeSet = ICorfuPayload.setFromBuffer(buf, String.class).stream()
                .map(CorfuMsgType::valueOf)
                .collect(Collectors.toSet());
        dropProbability = ICorfuPayload.fromBuffer(buf, Double.class);
        clearAllRules = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, corfuMsgTypeSet.stream().map(CorfuMsgType::toString).collect(Collectors.toSet()));
        ICorfuPayload.serialize(buf, dropProbability);
        ICorfuPayload.serialize(buf, clearAllRules);
    }
}
