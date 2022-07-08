package org.corfudb.common.config;

public final class ConfigParams {
    public static final String TLS_CIPHERS =
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" + ", " +
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384";

    private ConfigParams() {
        //prevent creating instances
    }
}
