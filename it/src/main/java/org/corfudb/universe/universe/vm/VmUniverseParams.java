package org.corfudb.universe.universe.vm;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.IpAddress;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents the parameters for constructing a VM {@link Universe}.
 */
@Getter
@ToString(callSuper = true)
public class VmUniverseParams extends UniverseParams {

    /**
     * Default https://10.173.65.98/sdk
     */
    @NonNull
    private final String vSphereUrl;

    /**
     * Default "10.172.208.208"
     */
    @NonNull
    private final List<String> vSphereHost;

    @NonNull
    private final VmCredentialsParams credentials;

    @NonNull
    private final String templateVMName;

    @NonNull
    private final ConcurrentMap<VmName, IpAddress> vmIpAddresses;
    @NonNull
    @Default
    private final String domainName = "eng.vmware.com";
    @NonNull
    @Default
    private final String timeZone = "America/Los_Angeles";
    @NonNull
    @Default
    private final String[] dnsServers = new String[]{"10.172.40.1", "10.172.40.2"};
    @NonNull
    @Default
    private final String[] domainSuffixes = new String[]{"eng.vmware.com", "vmware.com"};
    @NonNull
    @Default
    private final String[] gateways = new String[]{"10.172.211.253"};
    @NonNull
    @Default
    private final String subnet = "255.255.255.0";
    @NonNull
    @Default
    private final Duration readinessTimeout = Duration.ofSeconds(3);

    @Builder
    public VmUniverseParams(
            VmCredentialsParams credentials, String vSphereUrl, List<String> vSphereHost,
            String templateVMName, ConcurrentMap<VmName, IpAddress> vmIpAddresses, String networkName,
            boolean cleanUpEnabled) {
        super(networkName, new ConcurrentHashMap<>(), cleanUpEnabled);
        this.vSphereUrl = vSphereUrl;
        this.vSphereHost = vSphereHost;
        this.templateVMName = templateVMName;
        this.vmIpAddresses = vmIpAddresses;
        this.credentials = credentials;
    }


    public VmUniverseParams updateIpAddress(VmName vmName, IpAddress ipAddress) {
        vmIpAddresses.put(vmName, ipAddress);
        return this;
    }

    /**
     * VM credentials stored in a property file (vm.credentials.properties)
     * The file format:
     * <pre>
     * vsphere.username=vSphereUser
     * vsphere.password=vSpherePassword
     *
     * vm.username=vmPass
     * vm.password=vmUser
     * </pre>
     */
    @Builder
    @Getter
    public static class VmCredentialsParams {
        @NonNull
        private final Credentials vmCredentials;
        @NonNull
        private final Credentials vSphereCredentials;
    }

    @Builder
    @Getter
    public static class Credentials {
        @NonNull
        private final String username;
        @NonNull
        private final String password;
    }
}
