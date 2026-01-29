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
     * Default TLS protocols when --tls-protocols is not specified.
     * Restricts to TLS 1.2 and 1.3 only (avoids TLSv1.1 and older).
     */
    public static final String DEFAULT_TLS_PROTOCOLS_CSV = "TLSv1.2,TLSv1.3";

    /**
     * @return CSV values enabled TLS Ciphers from the enum
     */
    public static String getTlsCiphersCSV() {
        String enumValues = Arrays.toString(TlsCiphers.values());
        // remove '[' and ']'
        return enumValues.substring(1,enumValues.length()-1);
    }
}
