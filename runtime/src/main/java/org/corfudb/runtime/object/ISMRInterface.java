package org.corfudb.runtime.object;

import lombok.Data;

import java.util.Set;

/**
 * Created by mwei on 1/11/16.
 */
@Deprecated
public interface ISMRInterface {

    Set<SMRMethod> getSMRAccessors();

    Set<SMRMethod> getSMRMutators();

    Set<SMRMethod> getSMRMutatorAccessors();

    @Data
    class SMRMethod {
        final String methodName;
        final Class<?>[] argumentTypes;
        String entryName;

        public SMRMethod(String methodName, Class<?>[] argumentTypes) {
            this.methodName = methodName;
            this.argumentTypes = argumentTypes;
            this.entryName = null;
        }

        public SMRMethod(String methodName, Class<?>[] argumentTypes,
                         String entryName) {
            this.methodName = methodName;
            this.argumentTypes = argumentTypes;
            this.entryName = entryName;
        }
    }

}
