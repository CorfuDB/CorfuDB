package org.corfudb.universe.universe.vm;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents the parameters for constructing a VM {@link Universe}.
 */
@Getter
@ToString(callSuper = true)
public class VmUniverseParams extends UniverseParams {
    @NonNull
    private final String vSphereUrl;
    @NonNull
    private final String vSphereUsername;
    @NonNull
    private final String vSpherePassword;
    @NonNull
    private final String vSphereHost;
    @NonNull
    private final String templateVMName;
    @NonNull
    private final String vmUserName;
    @NonNull
    private final String vmPassword;
    @NonNull
    private final ConcurrentMap<String, String> vmIpAddresses;
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
    public VmUniverseParams(String vSphereUrl, String vSphereUsername, String vSpherePassword,
                            String vSphereHost, String templateVMName, String vmUserName, String vmPassword,
                            ConcurrentMap<String, String> vmIpAddresses, String networkName) {
        super(networkName, new ConcurrentHashMap<>());
        this.vSphereUrl = vSphereUrl;
        this.vSphereUsername = vSphereUsername;
        this.vSpherePassword = vSpherePassword;
        this.vSphereHost = vSphereHost;

        this.templateVMName = templateVMName;
        this.vmUserName = vmUserName;
        this.vmPassword = vmPassword;
        this.vmIpAddresses = vmIpAddresses;
    }


    public VmUniverseParams updateIpAddress(String vmName, String ipAddress) {
        vmIpAddresses.put(vmName, ipAddress);
        return this;
    }
}
