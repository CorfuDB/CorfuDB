package org.corfudb.security.tls;

import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class TlsTestContext {

    public static final Path CERT_DIR = Paths.get("src/test/resources/security/reload");
    public static final Path PASSWORD_FILE = CERT_DIR.resolve("password");

    public static final String CLIENT_TRUST_WITH_SERVER_FILE_NAME = "client_trust_with_server.jks";

    public static final Path DISABLE_CERT_EXPIRY_CHECK_FILE_NAME = CERT_DIR.resolve("DISABLE_CERT_EXPIRY_CHECK");

    public static final TrustStoreConfig SERVER_TRUST_WITH_CLIENT = buildTrustStore("server_trust_with_client.jks");
    public static final TrustStoreConfig SERVER_TRUST_NO_CLIENT = buildTrustStore("server_trust_no_client.jks");
    public static final TrustStoreConfig CLIENT_TRUST_WITH_SERVER = buildTrustStore(CLIENT_TRUST_WITH_SERVER_FILE_NAME);
    public static final TrustStoreConfig CLIENT_TRUST_NO_SERVER = buildTrustStore("client_trust_no_server.jks");

    public static final TrustStoreConfig FAKE_LOCATION_AND_PASS = TrustStoreConfig.from(
            "definitely fake location",
            "fake password"
    );
    public static final TrustStoreConfig FAKE_PASS = new TrustStoreConfig(
            CERT_DIR.resolve(CLIENT_TRUST_WITH_SERVER_FILE_NAME),
            CERT_DIR.resolve("fake-password"),
            DISABLE_CERT_EXPIRY_CHECK_FILE_NAME
    );

    public static final Path CLIENT_CERT = CERT_DIR.resolve("client.cert");
    public static final Path SERVER_CERT = CERT_DIR.resolve("server.cert");

    private TlsTestContext() {
        //prevent creating new instances
    }

    private static TrustStoreConfig buildTrustStore(String trustStoreFile) {
        return new TrustStoreConfig(
                CERT_DIR.resolve(trustStoreFile),
                PASSWORD_FILE,
                DISABLE_CERT_EXPIRY_CHECK_FILE_NAME
        );
    }
}
