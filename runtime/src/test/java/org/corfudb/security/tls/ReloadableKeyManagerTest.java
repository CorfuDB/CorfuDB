package org.corfudb.security.tls;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.X509KeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Netty's OpenSSL engine asks for server aliases using public key algorithm names
 * (EC, RSA, ...), while BCJSSE only answers to key exchange names (ECDHE_ECDSA, ...).
 */
public class ReloadableKeyManagerTest {

    private static final String ALIAS = "server";

    @Test
    public void testEcFallsBackToEcdheEcdsa() {
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA");
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), bcjse, "EC", null, null));
        assertEquals(Arrays.asList("EC", "ECDHE_ECDSA"), bcjse.asked);
    }

    @Test
    public void testEcEcFallsBackToEcdhEcdsa() {
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), new StubKeyManager("ECDH_ECDSA"), "EC_EC", null, null));
    }

    @Test
    public void testEcRsaFallsBackToEcdhRsa() {
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), new StubKeyManager("ECDH_RSA"), "EC_RSA", null, null));
    }

    @Test
    public void testDsaFallsBackToDheDsa() {
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), new StubKeyManager("DHE_DSA"), "DSA", null, null));
    }

    @Test
    public void testRsaFallbacksAreTriedInOrder() {
        StubKeyManager dheOnly = new StubKeyManager("DHE_RSA");
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), dheOnly, "RSA", null, null));
        assertEquals(Arrays.asList("RSA", "ECDHE_RSA", "DHE_RSA"), dheOnly.asked);

        StubKeyManager ecdheAndDhe = new StubKeyManager("ECDHE_RSA", "DHE_RSA");
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), ecdheAndDhe, "RSA", null, null));
        assertEquals(Arrays.asList("RSA", "ECDHE_RSA"), ecdheAndDhe.asked);
    }

    @Test
    public void testRequestedKeyTypeWinsWhenDelegateAcceptsIt() {
        // SunJSSE style delegate: knows "EC" and nothing else, so no fallback is attempted.
        StubKeyManager sunJsse = new StubKeyManager("EC");
        assertEquals(ALIAS, ReloadableKeyManager.chooseServerAlias(newLog(), sunJsse, "EC", null, null));
        assertEquals(Arrays.asList("EC"), sunJsse.asked);
    }

    @Test
    public void testNoAliasWhenKeyFamilyDoesNotMatch() {
        // An RSA only keystore must not be returned for an EC request.
        StubKeyManager rsaOnly = new StubKeyManager("RSA", "ECDHE_RSA", "DHE_RSA");
        assertNull(ReloadableKeyManager.chooseServerAlias(newLog(), rsaOnly, "EC", null, null));
        assertEquals(Arrays.asList("EC", "ECDHE_ECDSA"), rsaOnly.asked);

        // And an EC only keystore must not be returned for an RSA request.
        StubKeyManager ecOnly = new StubKeyManager("ECDHE_ECDSA");
        assertNull(ReloadableKeyManager.chooseServerAlias(newLog(), ecOnly, "RSA", null, null));
    }

    @Test
    public void testUnmappedKeyTypesAreNotRewritten() {
        // TLS 1.3 signature scheme style names and unknown types are passed through once.
        for (String keyType : Arrays.asList("RSASSA-PSS", "EdDSA", "ECDHE_ECDSA", "UNKNOWN")) {
            StubKeyManager none = new StubKeyManager();
            assertNull(ReloadableKeyManager.chooseServerAlias(newLog(), none, keyType, null, null));
            assertEquals(Arrays.asList(keyType), none.asked);
        }
    }

    @Test
    public void testIssuersAndSocketArePassedThrough() {
        Principal[] issuers = new Principal[0];
        Socket socket = new Socket();
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA");
        ReloadableKeyManager.chooseServerAlias(newLog(), bcjse, "EC", issuers, socket);
        assertEquals(2, bcjse.issuersSeen.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(issuers, bcjse.issuersSeen.get(i));
            assertEquals(socket, bcjse.socketsSeen.get(i));
        }
    }

    @Test
    public void testServerAliasesUseTheSameFallbacksAsChooseServerAlias() {
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA");
        assertArrayEquals(new String[]{ALIAS}, ReloadableKeyManager.getServerAliases(newLog(), bcjse, "EC", null));
        assertEquals(Arrays.asList("EC", "ECDHE_ECDSA"), bcjse.asked);

        StubKeyManager sunJsse = new StubKeyManager("EC");
        assertArrayEquals(new String[]{ALIAS}, ReloadableKeyManager.getServerAliases(newLog(), sunJsse, "EC", null));
        assertEquals(Arrays.asList("EC"), sunJsse.asked);
    }

    @Test
    public void testServerAliasesNullWhenNothingMatches() {
        StubKeyManager rsaOnly = new StubKeyManager("RSA", "ECDHE_RSA", "DHE_RSA");
        assertNull(ReloadableKeyManager.getServerAliases(newLog(), rsaOnly, "EC", null));
        assertEquals(Arrays.asList("EC", "ECDHE_ECDSA"), rsaOnly.asked);

        StubKeyManager none = new StubKeyManager();
        assertNull(ReloadableKeyManager.getServerAliases(newLog(), none, "RSASSA-PSS", null));
        assertEquals(Arrays.asList("RSASSA-PSS"), none.asked);
    }

    @Test
    public void testRepeatedOutcomeIsLoggedAtInfoOnceAcrossKeyManagerInstances() {
        // Corfu builds a new key manager per channel, so the memory must be shared between instances.
        Map<String, String> shared = new ConcurrentHashMap<>();
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA");

        try (LogCapture capture = new LogCapture()) {
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), bcjse, "EC", null, null);

            assertEquals(Arrays.asList(Level.INFO, Level.DEBUG, Level.DEBUG), capture.levels());
            assertTrue(capture.messages().get(0).contains("Chose server alias server for key type EC"));
        }
    }

    @Test
    public void testChangedOutcomeIsLoggedAtInfoAgain() {
        Map<String, String> shared = new ConcurrentHashMap<>();

        try (LogCapture capture = new LogCapture()) {
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), new StubKeyManager("ECDHE_ECDSA"), "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), new StubKeyManager(), "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks", 1), new StubKeyManager(), "EC", null, null);

            assertEquals(Arrays.asList(Level.INFO, Level.INFO, Level.DEBUG), capture.levels());
            assertTrue(capture.messages().get(1).contains("No server alias for key type EC"));
        }
    }

    @Test
    public void testKeyStoreReloadIsLoggedAtInfoAgain() {
        Map<String, String> shared = new ConcurrentHashMap<>();
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA");

        try (LogCapture capture = new LogCapture()) {
            ReloadableKeyManager.OutcomeLog log = newLog(shared, "ks", 1);
            ReloadableKeyManager.chooseServerAlias(log, bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(log, bcjse, "EC", null, null);

            // A new certificate was loaded: the same outcome is reported again.
            log.setKeyStoreVersion(2);
            ReloadableKeyManager.chooseServerAlias(log, bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(log, bcjse, "EC", null, null);

            assertEquals(Arrays.asList(Level.INFO, Level.DEBUG, Level.INFO, Level.DEBUG), capture.levels());
        }
    }

    @Test
    public void testOutcomesAreTrackedPerKeyStoreLookupKindAndKeyType() {
        Map<String, String> shared = new ConcurrentHashMap<>();
        StubKeyManager bcjse = new StubKeyManager("ECDHE_ECDSA", "ECDHE_RSA");

        try (LogCapture capture = new LogCapture()) {
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks1", 1), bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks2", 1), bcjse, "EC", null, null);
            ReloadableKeyManager.chooseServerAlias(newLog(shared, "ks1", 1), bcjse, "RSA", null, null);
            ReloadableKeyManager.getServerAliases(newLog(shared, "ks1", 1), bcjse, "EC", null);

            assertEquals(Arrays.asList(Level.INFO, Level.INFO, Level.INFO, Level.INFO), capture.levels());
            assertEquals(4, shared.size());
        }
    }

    private static ReloadableKeyManager.OutcomeLog newLog() {
        return newLog(new ConcurrentHashMap<>(), "ks", 1);
    }

    private static ReloadableKeyManager.OutcomeLog newLog(Map<String, String> shared, String keyStore, long version) {
        ReloadableKeyManager.OutcomeLog log = new ReloadableKeyManager.OutcomeLog(shared, keyStore);
        log.setKeyStoreVersion(version);
        return log;
    }

    /** Captures what ReloadableKeyManager logs, at any level. */
    private static final class LogCapture implements AutoCloseable {
        private final Logger logger = (Logger) LoggerFactory.getLogger(ReloadableKeyManager.class);
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        private final Level previousLevel = logger.getLevel();

        private LogCapture() {
            logger.setLevel(Level.DEBUG);
            appender.start();
            logger.addAppender(appender);
        }

        private List<Level> levels() {
            return appender.list.stream().map(ILoggingEvent::getLevel).collect(Collectors.toList());
        }

        private List<String> messages() {
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }
    }

    /** Answers with {@link #ALIAS} only for the key types it was built with, like BCJSSE does. */
    private static final class StubKeyManager implements X509KeyManager {
        private final List<String> accepted;
        private final List<String> asked = new ArrayList<>();
        private final List<Principal[]> issuersSeen = new ArrayList<>();
        private final List<Socket> socketsSeen = new ArrayList<>();

        private StubKeyManager(String... accepted) {
            this.accepted = Arrays.asList(accepted);
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            asked.add(keyType);
            issuersSeen.add(issuers);
            socketsSeen.add(socket);
            return accepted.contains(keyType) ? ALIAS : null;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return null;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            asked.add(keyType);
            return accepted.contains(keyType) ? new String[]{ALIAS} : null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return null;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return null;
        }
    }
}
