package org.corfudb.universe.universe.docker;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

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
public class FakeDns {
    private static FakeDns instance = new FakeDns();

    private final Map<String, InetAddress> forwardResolutions = new HashMap<>();

    private final Map<InetAddress, String> reverseResolutions = new HashMap<>();

    /**
     * whether the fake resolver has been installed
     */
    private boolean installed = false;

    private FakeDns() {
    }

    public static FakeDns getInstance() {
        return instance;
    }

    public synchronized void addForwardResolution(String hostname, InetAddress ip) {
        forwardResolutions.put(hostname, ip);
    }

    public synchronized void addReverseResolution(InetAddress ip, String hostname) {
        reverseResolutions.put(ip, hostname);
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

    private void installDns() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            ClassNotFoundException, NoSuchFieldException {
        try {
            // Override the NameService in Java 9 or later.
            final Class<?> nameServiceInterface = Class.forName("java.net.InetAddress$NameService");
            Field field = InetAddress.class.getDeclaredField("nameService");
            // Get the default NameService to fallback to.
            Method method = InetAddress.class.getDeclaredMethod("createNameService");
            method.setAccessible(true);
            Object fallbackNameService = method.invoke(null);
            // Create a proxy instance to set on the InetAddress field which will handle
            // all NameService calls.
            Object proxy = Proxy.newProxyInstance(
                    nameServiceInterface.getClassLoader(),
                    new Class<?>[]{nameServiceInterface},
                    new NameServiceListener(fallbackNameService)
            );
            field.setAccessible(true);
            field.set(InetAddress.class, proxy);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            // Override the NameService in Java 8 or earlier.
            final Class<?> nameServiceInterface = Class.forName("sun.net.spi.nameservice.NameService");
            Field field = InetAddress.class.getDeclaredField("nameServices");
            // Get the default NameService to fallback to.
            Method method = InetAddress.class.getDeclaredMethod("createNSProvider", String.class);
            method.setAccessible(true);
            Object fallbackNameService = method.invoke(null, "default");
            // Create a proxy instance to set on the InetAddress field which will handle
            // all NameService calls.
            Object proxy = Proxy.newProxyInstance(
                    nameServiceInterface.getClassLoader(),
                    new Class<?>[]{nameServiceInterface},
                    new NameServiceListener(fallbackNameService)
            );
            field.setAccessible(true);
            // Java 8 or earlier takes a list of NameServices
            field.set(InetAddress.class, Arrays.asList(proxy));
        }
    }

    /**
     * The NameService in all versions of Java has the same interface, so we
     * can use the same InvocationHandler as our proxy instance for both
     * java.net.InetAddress$NameService and sun.net.spi.nameservice.NameService.
     */
    private class NameServiceListener implements InvocationHandler {

        private final Object fallbackNameService;

        // Creates a NameServiceListener with a NameService implementation to
        // fallback to. The parameter is untyped so we can handle the NameService
        // type in all versions of Java with reflection.
        NameServiceListener(Object fallbackNameService) {
            this.fallbackNameService = fallbackNameService;
        }

        private InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException {
            InetAddress inetAddress;
            synchronized (FakeDns.this) {
                inetAddress = forwardResolutions.get(host);
            }
            if (inetAddress != null) {
                return new InetAddress[]{inetAddress};
            }

            try {
                Method method = fallbackNameService.getClass().getDeclaredMethod("lookupAllHostAddr", String.class);
                method.setAccessible(true);
                return (InetAddress[]) method.invoke(fallbackNameService, host);
            } catch (ReflectiveOperationException | NoSuchElementException | SecurityException e) {
                Throwables.propagateIfPossible(e.getCause(), UnknownHostException.class);
                throw new AssertionError("unexpected reflection issue", e);
            }
        }

        private String getHostByAddr(byte[] addr) throws UnknownHostException {
            if (addr[0] == 127) {
                return InetAddresses.toAddrString(InetAddress.getByAddress(addr));
            }

            String hostname;
            synchronized (FakeDns.this) {
                hostname = reverseResolutions.get(InetAddress.getByAddress(addr));
            }
            if (hostname != null) {
                return hostname;
            }

            try {
                Method method = fallbackNameService
                        .getClass()
                        .getDeclaredMethod("getHostByAddr", byte[].class);

                method.setAccessible(true);
                return (String) method.invoke(fallbackNameService, (Object) addr);
            } catch (ReflectiveOperationException | NoSuchElementException | SecurityException e) {
                Throwables.propagateIfPossible(e.getCause(), UnknownHostException.class);
                throw new AssertionError("unexpected reflection issue", e);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "lookupAllHostAddr":
                    return lookupAllHostAddr((String) args[0]);
                case "getHostByAddr":
                    return getHostByAddr((byte[]) args[0]);
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }
}
