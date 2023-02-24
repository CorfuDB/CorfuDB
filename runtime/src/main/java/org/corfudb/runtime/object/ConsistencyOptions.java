package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ConsistencyOptions {
    @Builder.Default
    private boolean readYourWrites = true;
}
