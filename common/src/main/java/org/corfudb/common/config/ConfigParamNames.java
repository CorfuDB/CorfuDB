package org.corfudb.common.config;

public final class ConfigParamNames {
    public static final String KEY_STORE = "--keystore";
    public static final String KEY_STORE_PASS_FILE = "--keystore-password-file";

    public static final String TRUST_STORE = "--truststore";
    public static final String TRUST_STORE_PASS_FILE = "--truststore-password-file";

    public static final String DISABLE_CERT_EXPIRY_CHECK_FILE = "--disable-cert-expiry-check-file";
    public static final String LAYOUT_RATE_LIMIT_TIMEOUT = "--layout-rate-limit-timeout";
    public static final String LAYOUT_RATE_LIMIT_RESET_MULTIPLIER = "--layout-rate-limit-reset-multiplier";
    public static final String LAYOUT_RATE_LIMIT_COOLDOWN_MULTIPLIER = "--layout-rate-limit-cooldown-multiplier";



    private ConfigParamNames() {
        //prevent creating instances
    }
}
