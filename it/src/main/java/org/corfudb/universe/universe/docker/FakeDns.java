package org.corfudb.universe.universe.docker;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Fake DNS resolver which allows our tests to work well even though we use
 * strange loopback IP addresses (127.x.y.z) with no corresponding reverse
 * DNS.
 * <p>
 * This overrides the reverse lookups for such IPs to return the same address
 * in String form.
 * <p>
 * Without this class, reverse DNS lookups for such addresses often take
 * 5 seconds to return, causing timeouts and overall test slowness.
 * <p>
 * In the future this class might also be extended to test more interesting
 * DNS-related scenarios.
 */
@Slf4j
public class FakeDns {
    private static final FakeDns instance = new FakeDns();
    private final Map<String, InetAddress> forwardResolutions = new HashMap<>();

    /**
     * whether the fake resolver has been installed
     */
    private boolean installed = false;

    private FakeDns() {
    }

    public static FakeDns getInstance() {
        return instance;
    }

    public synchronized FakeDns addForwardResolution(String hostname, InetAddress ip) {
        forwardResolutions.put(hostname, ip);
        return this;
    }

    /**
     * Install the fake DNS resolver into the Java runtime.
     */
    public synchronized FakeDns install() {
        if (installed) return this;
        try {
            installDns();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        installed = true;

        return this;
    }

    public class CorfuResolver implements InetAddressResolver {

        private final InetAddressResolver defaultResolver;

        public CorfuResolver(InetAddressResolver defaultResolver) {
            this.defaultResolver = defaultResolver;
        }

        @Override
        public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
            if (forwardResolutions.containsKey(host)) {
                return Stream.of(forwardResolutions.get(host));
            } else {
                return defaultResolver.lookupByName(host, lookupPolicy);
            }
        }

        @Override
        public String lookupByAddress(byte[] addr) throws UnknownHostException {
            return defaultResolver.lookupByAddress(addr);
        }
    }

    private void installDns() throws IllegalAccessException, NoSuchFieldException {
        Field resolverField = InetAddress.class.getDeclaredField("resolver");
        resolverField.setAccessible(true);

        var defaultResolver = (InetAddressResolver) resolverField.get(InetAddress.class);
        resolverField.set(InetAddressResolver.class, new CorfuResolver(defaultResolver));
    }
}
