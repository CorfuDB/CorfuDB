package org.corfudb.test;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.security.tls.TlsUtils;
import org.corfudb.util.Sleep;

import javax.net.ssl.SSLException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.corfudb.common.util.CertificateUtils.computeCertificateThumbprint;

/**
 * Created by zlokhandwala on 2019-06-06.
 */
public class TestUtils {

    /**
     * Sets aggressive timeouts for all the router endpoints on all the runtimes.
     * <p>
     *
     * @param layout        Layout to get all server endpoints.
     * @param corfuRuntimes All runtimes whose routers' timeouts are to be set.
     */
    public static void setAggressiveTimeouts(Layout layout, CorfuRuntime... corfuRuntimes) {
        layout.getAllServers().forEach(routerEndpoint -> {
            for (CorfuRuntime runtime : corfuRuntimes) {
                runtime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            }
        });
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuRuntime corfuRuntime) {
        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    /**
     * Read the keystore file and extract the certificates from the keystore
     * and compute their thumbprints
     * @param keyStoreConfig Keystore configuration with the jks file and its password
     * @return a list of thumbprints of the certificates in the keystore
     */
    public static List<String> extractThumbprintsFromKeyStore(TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig) throws SSLException {
        KeyStore keyStore = null;
        String keyStorePassword = null;
        keyStore = TlsUtils.openCertStore(keyStoreConfig);
        keyStorePassword = TlsUtils.getKeyStorePassword(keyStoreConfig.getPasswordFile());
        List<String> thumbprintList = new ArrayList<>();
        try {
            // Get an enumeration of all the aliases in the keystore
            Enumeration<String> aliases = keyStore.aliases();

            // Iterate through the aliases and print the information for each entry
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                KeyStore.Entry entry = keyStore.getEntry(alias, new KeyStore.PasswordProtection(keyStorePassword.toCharArray()));
                if (entry instanceof KeyStore.PrivateKeyEntry) {
                    KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;
                    thumbprintList.add(computeCertificateThumbprint(privateKeyEntry.getCertificate()));
                } else if (entry instanceof KeyStore.TrustedCertificateEntry) {
                    KeyStore.TrustedCertificateEntry trustedCertificateEntry = (KeyStore.TrustedCertificateEntry) entry;
                    thumbprintList.add(computeCertificateThumbprint(trustedCertificateEntry.getTrustedCertificate()));
                }
            }
        } catch (KeyStoreException | UnrecoverableEntryException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }

        return thumbprintList;
    }
}
