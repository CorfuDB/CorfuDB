package org.corfudb.universe.universe.vm;

import com.google.common.collect.ImmutableMap;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.vm.VmUniverseParams.Credentials;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Managing vm appliances: deploy vm's, stop, restart, destroy.
 * Keeps list of vm's
 */
@Builder
@Slf4j
public class ApplianceManager {

    private final ConcurrentMap<VmName, VmManager> vms = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @NonNull
    private final VmUniverseParams universeParams;

    public enum ResourceType {
        HOST_SYSTEM("HostSystem"),
        RESOURCE_TYPE("ResourcePool"),
        DATA_STORE("Datastore"),
        FOLDER("Folder");

        public final String resource;

        ResourceType(String resource) {
            this.resource = resource;
        }
    }

    /**
     * Deploy virtual machines, according to universe parameters
     */
    public void deploy() {
        log.info("Vm deployment");

        Map<VmName, VmManager> deployment = new HashMap<>();

        universeParams
                .getVmIpAddresses()
                .keySet()
                .stream()
                .map(this::deployVmAsync)
                //Must transform a stream to the list to prevent lazy evaluation
                .collect(Collectors.toList())
                .stream()
                .map(CompletableFuture::join)
                .forEach(vm -> {
                    universeParams.updateIpAddress(vm.getVmName(), vm.getResolvedIpAddress());
                    deployment.put(vm.getVmName(), vm);
                });

        log.info("The deployed VMs are: {}", universeParams.getVmIpAddresses());

        vms.putAll(deployment);
    }

    private CompletableFuture<VmManager> deployVmAsync(VmName vmName) {
        log.info("Deploy vm asynchronously: {}", vmName);
        return CompletableFuture.supplyAsync(() -> deployVm(vmName).get(), executor);
    }

    /**
     * Deploy and power on a VM appliance in vSphere.
     *
     * @return VirtualMachine instance
     */
    private Result<VmManager, UniverseException> deployVm(VmName vmName) {
        log.info("Deploy VM: {}", vmName);
        return getVmManager(vmName).flatMap(VmManager::setup);
    }

    private Result<VmManager, UniverseException> getVmManager(VmName vmName) {
        log.debug("Get vm manager for: {}", vmName);

        return Result.of(() -> {
            try {
                // Connect to vSphere server using VIJAVA
                Credentials vSphereCredentials = universeParams
                        .getCredentials()
                        .getVSphereCredentials();

                ServiceInstance si = new ServiceInstance(
                        new URL(universeParams.getVSphereUrl()),
                        vSphereCredentials.getUsername(),
                        vSphereCredentials.getPassword(),
                        true
                );

                return VmManager.builder()
                        .vmName(vmName)
                        .navigator(new InventoryNavigator(si.getRootFolder()))
                        .universeParams(universeParams)
                        .build();
            } catch (Exception ex) {
                throw new UniverseException("Can't init VM Manager", ex);
            }
        });
    }

    public ImmutableMap<VmName, VmManager> getVms() {
        return ImmutableMap.copyOf(vms);
    }

    enum ManagedEntityType {
        VIRTUAL_MACHINE("VirtualMachine");

        public final String typeName;

        ManagedEntityType(String typeName) {
            this.typeName = typeName;
        }
    }

}
