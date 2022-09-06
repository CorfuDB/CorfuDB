package org.corfudb.runtime.clients;

import lombok.AllArgsConstructor;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.CertManagementConfig;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;
import sun.security.tools.keytool.CertAndKeyGen;
import sun.security.x509.X500Name;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.Random;

import static org.assertj.core.api.Assertions.fail;

public interface NettyCommTestUtil {
    String PASSWORD = "test123";
    String PASSWORD_FILE = "src/test/resources/security/storepass";

    /**
     * <a href="https://www.baeldung.com/java-keystore">JKS Documentation</a>
     * <a href="https://www.baeldung.com/java-keystore-convert-to-pem-format">Pem format</a>
     */
    @AllArgsConstructor
    class CertificateManager {
        private static final Random RND = new Random();

        public final CertAndKeyGen gen;
        public final X500Name certName;
        public final X509Certificate[] chain;
        public final X509Certificate cert;
        public final KeyStore keyStore;
        public final String password;
        public final String alias;
        public final Path jksPath;
        public final KeyStoreConfig keyStoreConfig;
        public final TrustStoreManager trustStoreManager;
        public final CertManagementConfig certManagementConfig;

        public static CertificateManager buildSHA384withEcDsa(Path certDir) throws Exception {
            final int aliasNameLength = 6;
            final int validDays = 30;
            final int oneDay = 1;
            return buildSHA384withEcDsa(
                    certDir,
                    RandomStringUtils.randomAlphabetic(aliasNameLength),
                    Duration.ofDays(validDays),
                    new Date(System.currentTimeMillis() - Duration.ofDays(oneDay).toMillis())
            );
        }

        public static CertificateManager buildSHA384withEcDsa(Path certDir, String alias, Duration validity,
                                                              Date firstDate) throws Exception {

            final String keyType = "EC";
            final String sigAlg = "SHA384withECDSA";
            return build(keyType, sigAlg, PASSWORD, certDir, alias, validity, firstDate);
        }

        public static CertificateManager build(
                String keyType, String sigAlg, String password, Path certDir, String alias,
                Duration validity, Date firstDate) throws Exception {

            Path jksPath = certDir.resolve(String.format("keystore-%s.jks", generateRandom()));

            CertAndKeyGen gen = new CertAndKeyGen(keyType, sigAlg);
            X500Name certName = new X500Name("CN=ROOT");
            final int keyBits = 384;
            gen.generate(keyBits);

            X509Certificate cert = gen.getSelfCertificate(certName, firstDate, validity.getSeconds());
            X509Certificate[] chain = new X509Certificate[1];
            chain[0] = cert;

            KeyStore keyStore = KeyStore.getInstance("JKS");
            if (Files.exists(jksPath)) {
                throw new IllegalStateException("Trust store file already exists");
            } else {
                keyStore.load(null, null);
            }

            PrivateKey privateKey = gen.getPrivateKey();
            keyStore.setKeyEntry(alias, privateKey, password.toCharArray(), chain);
            try (OutputStream storeFile = Files.newOutputStream(jksPath)) {
                keyStore.store(storeFile, password.toCharArray());
            }

            KeyStoreConfig keyStoreConfig = new KeyStoreConfig(
                    jksPath,
                    Paths.get(PASSWORD_FILE)
            );

            Path trustStorePath = certDir.resolve("truststore-" + generateRandom() + ".jks");
            TrustStoreManager trustStoreManager = TrustStoreManager.build(trustStorePath);
            trustStoreManager.trustStore.setCertificateEntry(alias, cert);
            trustStoreManager.save();

            CertManagementConfig cfg = new CertManagementConfig(
                    keyStoreConfig,
                    trustStoreManager.config
            );

            return new CertificateManager(
                    gen, certName, chain, cert, keyStore, password, alias, jksPath, keyStoreConfig,
                    trustStoreManager, cfg
            );
        }

        private static int generateRandom() {
            final int bound = 100_000;
            return RND.nextInt(bound);
        }

        /**
         * Serialize and save public key certificate in pem format
         */
        public void saveCertificate() throws Exception {
            String encodedCert = Base64.getEncoder().encodeToString(cert.getEncoded());
            String certPem = "-----BEGIN CERTIFICATE-----\n" +
                    encodedCert +
                    "\n-----END CERTIFICATE-----\n";

            Files.write(Paths.get(AbstractCorfuTest.PARAMETERS.TEST_TEMP_DIR, alias + ".cert"), certPem.getBytes());
        }

        public TrustStoreConfig keyStoreConfigAsTrustStore() {
            return new TrustStoreConfig(
                    keyStoreConfig.getKeyStoreFile(),
                    keyStoreConfig.getPasswordFile(),
                    TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE
            );
        }
    }

    @AllArgsConstructor
    class TrustStoreManager {
        public final KeyStore trustStore;
        public final Path truststorePath;
        public final String password;

        public final TrustStoreConfig config;

        /**
         * <a href="https://stackoverflow.com/questions/24555890/using-a-custom-truststore-in-java-as-well-as-the-default-one">TrustStore Information</a>
         *
         * @return trust store manager
         * @throws Exception error
         */
        public static TrustStoreManager build(Path trustStorePath) throws Exception {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            if (Files.exists(trustStorePath)) {
                fail("Trust store file: " + trustStorePath + " already exists");
            }

            trustStore.load(null, null);

            TrustStoreConfig config = new TrustStoreConfig(
                    trustStorePath,
                    Paths.get(PASSWORD_FILE),
                    TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE
            );

            return new TrustStoreManager(trustStore, trustStorePath, PASSWORD, config);
        }

        public void addCertificate(CertificateManager certManager) throws Exception {
            trustStore.setCertificateEntry(certManager.alias, certManager.cert);
        }

        public void save() throws Exception {
            try (OutputStream truststoreFile = Files.newOutputStream(truststorePath)) {
                trustStore.store(truststoreFile, password.toCharArray());
            }
        }
    }
}
