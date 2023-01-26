package org.corfudb.infrastructure.health;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@AllArgsConstructor
public class LivenessStatus {
    @Getter
    boolean isHealthy;
    @Getter
    String reason;
}
