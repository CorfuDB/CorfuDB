package org.corfudb.universe.universe.vm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.TaskInfo;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.vm.ApplianceManager.ManagedEntityType;
import org.corfudb.universe.universe.vm.ApplianceManager.ResourceType;
import org.corfudb.universe.util.IpAddress;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.DATA_STORE;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.HOST_SYSTEM;
import static org.corfudb.universe.universe.vm.ApplianceManager.ResourceType.RESOURCE_TYPE;

/**
 * Virtual machine manager
 */
@Builder
@Slf4j
public class VmManager {

    @NonNull
    @Getter
    private final VmName vmName;

    /**
     * Vsphere inventory navigator
     */
    @NonNull
    private final InventoryNavigator navigator;

    @NonNull
    private final VmUniverseParams universeParams;

    /**
     * Clone a vm (if needed) and setup it
     *
     * @return vm manager
     */
    public Result<VmManager, UniverseException> setup() {
        log.info("Setup vm: {}", vmName);

        return findVm(vmName.getName()).flatMap(this::setupAction);
    }

    private Result<VmManager, UniverseException> setupAction(Optional<VirtualMachine> maybeVm) {
        return maybeVm
                .map(this::cloneAndPowerOn)
                //If a vm not found then clone the vm from a templateVm
                .orElseGet(() -> cloneVm().map(taskInfo -> this));
    }

    public Result<Void, UniverseException> shutdown() {
        return getVm().flatMap(vm -> Result.of(() -> {
            try {
                vm.shutdownGuest();
            } catch (Exception e) {
                throw new UniverseException(e);
            }

            return null;
        }));
    }

    public Result<Void, UniverseException> reboot() {
        return getVm().flatMap(vm -> Result.of(() -> {
            try {
                vm.rebootGuest();
            } catch (Exception e) {
                throw new UniverseException(e);
            }

            return null;
        }));
    }

    public Result<TaskInfo, UniverseException> reset() {
        return getVm().flatMap(vm -> executeTask(vm::resetVM_Task));
    }

    public Result<TaskInfo, UniverseException> powerOff() {
        return getVm().flatMap(vm -> executeTask(vm::powerOffVM_Task));
    }

    public Result<TaskInfo, UniverseException> destroy() {
        return getVm().flatMap(vm -> executeTask(vm::destroy_Task));
    }

    public CompletableFuture<IpAddress> getIpAddress() {
        log.info("Getting IP address for {} from DHCP...please wait...", vmName);

        return CompletableFuture.supplyAsync(() -> {
            Optional<String> ipAddress = Optional.empty();
            while (!ipAddress.isPresent()) {
                log.info("Waiting for ip address. Vm: {}", vmName);

                try {
                    TimeUnit.SECONDS.sleep(universeParams.getReadinessTimeout().getSeconds());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UniverseException("Error ", e);
                }

                ipAddress = getVm()
                        .flatMap(vm -> Result.of(vm::getGuest))
                        .map(GuestInfo::getIpAddress)
                        .map(Optional::of)
                        .orElse(Optional.empty());
            }

            return IpAddress.builder().ip(ipAddress.get()).build();
        });
    }

    public IpAddress getResolvedIpAddress() {
        return getIpAddress().join();
    }

    public Result<TaskInfo, UniverseException> powerOn() {
        log.info("Power on vm: {}", vmName);

        return getVm().flatMap(vm -> executeTask(() -> {
            log.info("Powering on the vm: {} ...", vmName);

            HostSystem hostSystem = (HostSystem) navigator
                    .searchManagedEntity(HOST_SYSTEM.resource, "host");

            return vm.powerOnVM_Task(hostSystem);
        }));
    }

    public Result<VirtualMachine, UniverseException> getVm() {
        return getVm(vmName.getName());
    }

    private Result<VirtualMachine, UniverseException> getVm(String name) {
        return findVm(name).flatMap(maybeVm -> {
            if (maybeVm.isPresent()) {
                return Result.ok(maybeVm.get());
            } else {
                return Result.error(vmNotFoundError());
            }
        });
    }

    /**
     * Search for a virtual machine in a VSphere cluster
     *
     * @return a virtual machine
     */
    private Result<Optional<VirtualMachine>, UniverseException> findVm(String name) {
        log.debug("Find vm: {}", name);

        return Result.of(() -> {
            try {
                VirtualMachine vm = (VirtualMachine) navigator.searchManagedEntity(
                        ManagedEntityType.VIRTUAL_MACHINE.typeName, name
                );
                return Optional.ofNullable(vm);
            } catch (Exception e) {
                throw new UniverseException("Can't find a vm: " + vmName, e);
            }
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

    /**
     * Clone and powerOn a vm
     *
     * @return result of a clone task
     */
    public Result<TaskInfo, UniverseException> cloneVm() {
        log.info("Clone vm: {}", vmName);

        // Find the template machine in the inventory and clone/create the vm from vmTemplate
        return getVm(universeParams.getTemplateVMName()).flatMap(vmTemplate -> {
            log.info("Cloning the VM {} via vSphere {}", vmName, universeParams.getVSphereUrl());

            try {

                Properties vmPropsResult = VmConfigPropertiesLoader
                        .loadVmProperties()
                        .get();

                ImmutableMap<String, String> vmProps = Maps.fromProperties(vmPropsResult);

                // Create customization for cloning process
                VirtualMachineCloneSpec cloneSpec = createLinuxCustomization(vmProps);

                String folderProp = String.format(
                        "vm%d.%s", vmName.getIndex(), ResourceType.FOLDER.resource
                );

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

                return executeTask(() -> vmTemplate.cloneVM_Task(folder, vmName.getName(), cloneSpec));
            } catch (RuntimeException ex) {
                return Result.error(new UniverseException(ex));
            }
        });
    }

    /**
     * Create the Linux customization for a specific VM to be cloned.
     *
     * @return VirtualMachineCloneSpec instance
     */
    private VirtualMachineCloneSpec createLinuxCustomization(ImmutableMap<String, String> props) {
        log.info("Create linux customization for: {}", vmName);
        int index = vmName.getIndex();

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

    private Result<VmManager, UniverseException> cloneAndPowerOn(VirtualMachine vm) {
        try {
            VirtualMachinePowerState powerState = vm.getSummary().runtime.powerState;
            if (powerState == VirtualMachinePowerState.poweredOn) {
                return Result.ok(this);
            } else {
                return powerOn().map(taskInfo -> this);
            }
        } catch (RuntimeException ex) {
            return Result.error(new UniverseException(ex));
        }
    }

    private UniverseException vmNotFoundError() {
        return new UniverseException("A vm not found: " + vmName);
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
