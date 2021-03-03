package org.corfudb.universe.node.server.vm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.corfudb.universe.node.server.CorfuServerParams;


/**
 * Represents the parameters for constructing a {@link VmCorfuServer}.
 */
@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class VmCorfuServerParams extends CorfuServerParams {
    @NonNull
    private final VmName vmName;

    @Builder
    @EqualsAndHashCode
    @Getter
    public static class VmName implements Comparable<VmName> {
        /**
         * Vm name in a vSphere cluster
         */
        @NonNull
        private final String name;

        /**
         * Vm index in a vm.properties config
         */
        @NonNull
        private final Integer index;

        @Override
        public int compareTo(VmName other) {
            return name.compareTo(other.name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
