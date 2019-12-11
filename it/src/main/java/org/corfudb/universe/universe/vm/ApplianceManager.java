package org.corfudb.universe.universe.vm;

import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.DATA_STORE;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.HOST_SYSTEM;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.RESOURCE_TYPE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.LocalizedMethodFault;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.TaskInfo;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.corfudb.common.result.Result;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.vm.VmUniverseParams.Credentials;
import org.corfudb.universe.util.IpAddress;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    enum ResourceType {
        HOST_SYSTEM("HostSystem"),
        RESOURCE_TYPE("ResourcePool"),
        DATA_STORE("Datastore"),
        FOLDER("Folder");

        private final String resource;

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

        Stream<VmName> ipAddressStream = universeParams.getVmIpAddresses().keySet().stream();

        List<CompletableFuture<VmManager>> asyncDeployment = Streams
                .mapWithIndex(ipAddressStream, ImmutablePair::new)
                .map(pair -> deployVmAsync(pair.left, pair.right))
                .collect(Collectors.toList());

        asyncDeployment
                .stream()
                .map(CompletableFuture::join)
                .forEach(vm -> {
                    universeParams.updateIpAddress(vm.getVmName(), vm.getResolvedIpAddress());
                    deployment.put(vm.getVmName(), vm);
                });

        log.info("The deployed VMs are: {}", universeParams.getVmIpAddresses());

        vms.putAll(deployment);
    }

    private CompletableFuture<VmManager> deployVmAsync(VmName vmName, long index) {
        log.info("Deploy vm asynchronously: {}", vmName);
        return CompletableFuture.supplyAsync(() -> deployVm(vmName, index), executor);
    }

    /**
     * Deploy and power on a VM appliance in vSphere.
     *
     * @return VirtualMachine instance
     */
    private VmManager deployVm(VmName vmName, long index) {
        log.info("Deploy VM: {}", vmName);

        VmManager vmManager;
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

            InventoryNavigator inventoryNavigator = new InventoryNavigator(si.getRootFolder());

            // First check if a VM with this name already exists or not
            VirtualMachine vm = (VirtualMachine) inventoryNavigator.searchManagedEntity(
                    ManagedEntityType.VIRTUAL_MACHINE.typeName, vmName.getHost()
            );

            if (vm == null) {
                // Find the template machine in the inventory
                VirtualMachine vmTemplate = (VirtualMachine) inventoryNavigator.searchManagedEntity(
                        ManagedEntityType.VIRTUAL_MACHINE.typeName,
                        universeParams.getTemplateVMName()
                );

                log.info("Deploying the VM {} via vSphere {}...",
                        vmName, universeParams.getVSphereUrl());

                Properties vmPropsResult = VmConfigPropertiesLoader
                        .loadVmProperties()
                        .get();

                ImmutableMap<String, String> vmProps = Maps.fromProperties(vmPropsResult);

                // Create customization for cloning process
                VirtualMachineCloneSpec cloneSpec = createLinuxCustomization(vmProps, index);

                String folderProp = String.format("vm%d.%s", index, ResourceType.FOLDER.resource);

                Folder folder;
                if (vmProps.containsKey(folderProp)) {
                    ManagedObjectReference folderR = new ManagedObjectReference();
                    folderR.setType(ResourceType.FOLDER.resource);

                    String prop = vmProps.get(folderProp);
                    folderR.setVal(prop);

                    folder = new Folder(vmTemplate.getServerConnection(), folderR);
                } else {
                    folder = (Folder) vmTemplate.getParent();
                }

                Optional<LocalizedMethodFault> cloneErr = Optional.empty();
                try {
                    // Do the cloning - providing the clone specification
                    Task cloneTask = vmTemplate.cloneVM_Task(folder, vmName.getHost(), cloneSpec);
                    cloneTask.waitForTask();

                    cloneErr = Optional.ofNullable(cloneTask.getTaskInfo().getError());
                } catch (Exception e) {
                    String err = String.format("Deploy VM %s failed due to ", vmName);
                    throw new UniverseException(err, e);
                }

                cloneErr.ifPresent(err -> {
                    throw new UniverseException(err.getLocalizedMessage());
                });

                // After the clone task completes, get the VM from the inventory
                vm = (VirtualMachine) inventoryNavigator.searchManagedEntity(
                        ManagedEntityType.VIRTUAL_MACHINE.typeName, vmName.getHost()
                );
            }

            vmManager = VmManager.builder()
                    .vm(vm)
                    .vmName(vmName)
                    .serviceInstance(si)
                    .build();

            vmManager.powerOn().get();
            vmManager.getIpAddress().join();

        } catch (Exception e) {
            String err = String.format("Deploy VM %s failed due to ", vmName);
            throw new UniverseException(err, e);
        }

        return vmManager;
    }


    /**
     * Create the Linux customization for a specific VM to be cloned.
     *
     * @return VirtualMachineCloneSpec instance
     */
    private VirtualMachineCloneSpec createLinuxCustomization(
            ImmutableMap<String, String> props, long index) {

        VirtualMachineCloneSpec vmCloneSpec = new VirtualMachineCloneSpec();

        String hostKey = String.format("vm%d.%s", index, HOST_SYSTEM.resource);
        String dsKey = String.format("vm%d.%s", index, DATA_STORE.resource);
        String resourceTypeKey = String.format("vm%d.%s", index, RESOURCE_TYPE.resource);

        //Set location of clone to be the same as template (Datastore)
        VirtualMachineRelocateSpec vmRelocateSpec = new VirtualMachineRelocateSpec();

        if (props.keySet().containsAll(Arrays.asList(hostKey, dsKey, resourceTypeKey))) {
            ManagedObjectReference host = new ManagedObjectReference();
            host.setType(HOST_SYSTEM.resource);
            host.setVal(props.get(hostKey));
            vmRelocateSpec.setHost(host);

            ManagedObjectReference dataStore = new ManagedObjectReference();
            dataStore.setType(DATA_STORE.resource);
            dataStore.setVal(props.get(dsKey));
            vmRelocateSpec.setDatastore(dataStore);

            ManagedObjectReference pool = new ManagedObjectReference();
            pool.setType(RESOURCE_TYPE.resource);
            pool.setVal(props.get(resourceTypeKey));
            vmRelocateSpec.setPool(pool);
        }

        vmCloneSpec.setLocation(vmRelocateSpec);

        //Clone is powered on, not a template.
        vmCloneSpec.setPowerOn(true);
        vmCloneSpec.setTemplate(false);
        return vmCloneSpec;
    }

    public ImmutableMap<VmName, VmManager> getVms() {
        return ImmutableMap.copyOf(vms);
    }

    enum ManagedEntityType {
        VIRTUAL_MACHINE("VirtualMachine");

        private final String typeName;

        ManagedEntityType(String typeName) {
            this.typeName = typeName;
        }
    }

    @Builder
    @Slf4j
    public static class VmManager {
        @NonNull
        @Getter
        private final VmName vmName;

        @NonNull
        @Getter
        private final VirtualMachine vm;

        @NonNull
        private final ServiceInstance serviceInstance;

        @NonNull
        private final VmUniverseParams universeParams;

        public Result<Void, UniverseException> shutdown() {
            return Result.of(() -> {
                try {
                    vm.shutdownGuest();
                } catch (RemoteException e) {
                    throw new UniverseException(e);
                }

                return null;
            });
        }

        public Result<Void, UniverseException> reboot() {
            return Result.of(() -> {
                try {
                    vm.rebootGuest();
                } catch (RemoteException e) {
                    throw new UniverseException(e);
                }

                return null;
            });
        }

        public Result<TaskInfo, UniverseException> reset() {
            return executeTask(vm::resetVM_Task);
        }

        public Result<TaskInfo, UniverseException> powerOff() {
            return executeTask(vm::powerOffVM_Task);
        }

        public Result<TaskInfo, UniverseException> destroy() {
            return executeTask(vm::destroy_Task);
        }

        public CompletableFuture<IpAddress> getIpAddress() {
            log.info("Getting IP address for {} from DHCP...please wait...", vmName);

            CompletableFuture<IpAddress> result = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() -> {
                IpAddress ipAddress = null;
                while (ipAddress == null) {
                    log.info("Waiting for ip address. Vm: {}", vmName);

                    try {
                        TimeUnit.SECONDS.sleep(universeParams.getReadinessTimeout().getSeconds());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UniverseException("Error ", e);
                    }
                    GuestInfo guest = vm.getGuest();
                    ipAddress = IpAddress.builder().ip(guest.getIpAddress()).build();
                }

                return ipAddress;
            }).whenComplete((ip, ex) -> {
                if(ex != null) {
                    result.completeExceptionally(ex);
                }

                result.complete(ip);
            });

            return result;
        }

        public IpAddress getResolvedIpAddress() {
            return IpAddress.builder().ip(vm.getGuest().getIpAddress()).build();
        }

        public Result<TaskInfo, UniverseException> powerOn() {
            return executeTask(() -> {
                // If the VM already exists, ensure the power state is 'on' before return the VM
                VirtualMachinePowerState powerState = vm.getSummary().runtime.powerState;
                if (powerState != VirtualMachinePowerState.poweredOff) {
                    String err = "Can't power on a vm: " + vmName + ", state: " + powerState;
                    throw new UniverseException(err);
                }

                log.info("{} already exists, but found in 'poweredOff' state, powering it on...",
                        vmName);

                InventoryNavigator inventoryNavigator = new InventoryNavigator(
                        serviceInstance.getRootFolder()
                );
                HostSystem hostSystem = (HostSystem) inventoryNavigator
                        .searchManagedEntity(HOST_SYSTEM.resource, "host");

                return vm.powerOnVM_Task(hostSystem);

            });
        }

        private Result<TaskInfo, UniverseException> executeTask(VmSupplier<Task> action) {
            return Result.<TaskInfo, UniverseException>of(() -> {
                try {
                    Task task = action.get();
                    task.waitForTask();
                    return task.getTaskInfo();
                } catch (UniverseException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new UniverseException(ex);
                }
            }).map(taskInfo -> {
                if (taskInfo.getError() != null) {
                    throw new UniverseException(taskInfo.getError().getLocalizedMessage());
                }

                return taskInfo;
            });
        }

        @FunctionalInterface
        public interface VmSupplier<T> {

            /**
             * Gets a result.
             *
             * @return a result
             */
            T get() throws Exception;
        }
    }
}
