package org.corfudb.common.config;

import java.util.Arrays;

public final class ConfigParamsHelper {
    public enum TlsCiphers {
        TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
    }

    private ConfigParamsHelper() {
        //prevent creating instances
    }

    /**
     * @return CSV values enabled TLS Ciphers from the enum
     */
    public static String getTlsCiphersCSV() {
        String enumValues = Arrays.toString(TlsCiphers.values());
        // remove '[' and ']'
        return enumValues.substring(1,enumValues.length()-1);
    }
}
