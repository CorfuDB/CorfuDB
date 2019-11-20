package org.corfudb.universe.universe.vm;

import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.DATA_STORE;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.HOST_SYSTEM;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.RESOURCE_TYPE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.vmware.vim25.CustomizationAdapterMapping;
import com.vmware.vim25.CustomizationDhcpIpGenerator;
import com.vmware.vim25.CustomizationFixedName;
import com.vmware.vim25.CustomizationGlobalIPSettings;
import com.vmware.vim25.CustomizationIPSettings;
import com.vmware.vim25.CustomizationLinuxOptions;
import com.vmware.vim25.CustomizationLinuxPrep;
import com.vmware.vim25.CustomizationSpec;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.ManagedObjectReference;
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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.corfudb.common.result.Result;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.vm.VmConfigPropertiesLoader.PropsLoaderException;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final ConcurrentMap<String, VirtualMachine> vms = new ConcurrentHashMap<>();
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

        Map<String, VirtualMachine> deployment = new HashMap<>();

        Stream<String> ipAddressStream = universeParams.getVmIpAddresses().keySet().stream();
        List<CompletableFuture<VirtualMachine>> asyncDeployment = Streams
                .mapWithIndex(ipAddressStream, ImmutablePair::new)
                .map(pair -> deployVmAsync(pair.left, pair.right))
                .collect(Collectors.toList());

        asyncDeployment
                .stream()
                .map(CompletableFuture::join)
                .forEach(vm -> {
                    universeParams.updateIpAddress(vm.getName(), vm.getGuest().getIpAddress());
                    deployment.put(vm.getName(), vm);
                });

        log.info("The deployed VMs are: {}", universeParams.getVmIpAddresses());

        vms.putAll(deployment);
    }

    private CompletableFuture<VirtualMachine> deployVmAsync(String vmName, long index) {
        log.info("Deploy vm asynchronously: {}", vmName);
        return CompletableFuture.supplyAsync(() -> deployVm(vmName, index), executor);
    }

    /**
     * Deploy and power on a VM appliance in vSphere.
     *
     * @return VirtualMachine instance
     */
    private VirtualMachine deployVm(String vmName, long index) {
        log.info("Deploy VM: {}", vmName);

        VirtualMachine vm;
        try {
            // Connect to vSphere server using VIJAVA
            VmUniverseParams.Credentials vSphereCredentials = universeParams
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
            vm = (VirtualMachine) inventoryNavigator
                    .searchManagedEntity(ManagedEntityType.VIRTUAL_MACHINE.typeName, vmName);

            if (vm != null) {
                // If the VM already exists, ensure the power state is 'on' before return the VM
                if (vm.getSummary().runtime.powerState == VirtualMachinePowerState.poweredOff) {
                    log.info("{} already exists, but found in 'poweredOff' state, powering it on...",
                            vmName);

                    Task task = vm.powerOnVM_Task((HostSystem) inventoryNavigator
                            .searchManagedEntity(HOST_SYSTEM.resource, "host"));

                    task.waitForTask();
                }
            } else {
                // Find the template machine in the inventory
                VirtualMachine vmTemplate = (VirtualMachine) inventoryNavigator.searchManagedEntity(
                        ManagedEntityType.VIRTUAL_MACHINE.typeName,
                        universeParams.getTemplateVMName()
                );

                log.info("Deploying the VM {} via vSphere {}...",
                        vmName, universeParams.getVSphereUrl());

                Result<Properties, PropsLoaderException> vmPropsResult = VmConfigPropertiesLoader
                        .loadVmProperties();
                if (vmPropsResult.isError()) {
                    throw vmPropsResult.getError();
                }

                ImmutableMap<String, String> props = Maps.fromProperties(vmPropsResult.get());

                // Create customization for cloning process
                VirtualMachineCloneSpec cloneSpec = createLinuxCustomization(vmName, props, index);

                ManagedObjectReference folderR = new ManagedObjectReference();
                folderR.setType(ResourceType.FOLDER.resource);

                String propName = String.format("vm%d.%s", index, ResourceType.FOLDER.resource);
                String prop = props.get(propName);
                folderR.setVal(prop);

                Folder folder = new Folder(vmTemplate.getServerConnection(), folderR);
                Task cloneTask = vmTemplate.cloneVM_Task(folder, vmName, cloneSpec);

                try {
                    // Do the cloning - providing the clone specification
                    cloneTask.waitForTask();
                } catch (RemoteException | InterruptedException e) {
                    String err = String.format("Deploy VM %s failed due to ", vmName);
                    throw new UniverseException(err, e);
                }

                if (cloneTask.getTaskInfo().getError() != null) {
                    throw new UniverseException(
                            cloneTask.getTaskInfo().getError().getLocalizedMessage());
                }
                // After the clone task completes, get the VM from the inventory
                vm = (VirtualMachine) inventoryNavigator
                        .searchManagedEntity(ManagedEntityType.VIRTUAL_MACHINE.typeName, vmName);
            }
            // Ensure we get the VM's IP address before we return its instance
            String cloneVmIpAddress = null;
            log.info("Getting IP address for {} from DHCP...please wait...", vmName);
            while (cloneVmIpAddress == null) {
                log.info("Waiting for ip address. Vm: {}", vmName);
                TimeUnit.SECONDS.sleep(universeParams.getReadinessTimeout().getSeconds());
                GuestInfo guest = vm.getGuest();
                cloneVmIpAddress = guest.getIpAddress();
            }
        } catch (RemoteException | MalformedURLException | InterruptedException e) {
            Thread.currentThread().interrupt();
            String err = String.format("Deploy VM %s failed due to ", vmName);
            throw new UniverseException(err, e);
        }
        return vm;
    }


    /**
     * Create the Linux customization for a specific VM to be cloned.
     *
     * @return VirtualMachineCloneSpec instance
     */
    private VirtualMachineCloneSpec createLinuxCustomization(
            String cloneName, ImmutableMap<String, String> props, long index) {

        VirtualMachineCloneSpec vmCloneSpec = new VirtualMachineCloneSpec();

        //Set location of clone to be the same as template (Datastore)
        VirtualMachineRelocateSpec vmRelocateSpec = new VirtualMachineRelocateSpec();

        ManagedObjectReference host = new ManagedObjectReference();
        host.setType(HOST_SYSTEM.resource);
        host.setVal(props.get(String.format("vm%d.%s", index, HOST_SYSTEM.resource)));
        vmRelocateSpec.setHost(host);

        ManagedObjectReference dataStore = new ManagedObjectReference();
        dataStore.setType(DATA_STORE.resource);
        dataStore.setVal(props.get(String.format("vm%d.%s", index, DATA_STORE.resource)));
        vmRelocateSpec.setDatastore(dataStore);

        ManagedObjectReference pool = new ManagedObjectReference();
        pool.setType(RESOURCE_TYPE.resource);
        pool.setVal(props.get(String.format("vm%d.%s", index, RESOURCE_TYPE.resource)));
        vmRelocateSpec.setPool(pool);

        vmCloneSpec.setLocation(vmRelocateSpec);

        //Clone is powered on, not a template.
        vmCloneSpec.setPowerOn(true);
        vmCloneSpec.setTemplate(false);
        return vmCloneSpec;
    }

    public ImmutableMap<String, VirtualMachine> getVms() {
        return ImmutableMap.copyOf(vms);
    }

    enum ManagedEntityType {
        VIRTUAL_MACHINE("VirtualMachine");

        private final String typeName;

        ManagedEntityType(String typeName) {
            this.typeName = typeName;
        }
    }
}
