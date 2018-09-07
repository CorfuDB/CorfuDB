package org.corfudb.universe.cluster.vm;

import com.vmware.vim25.*;
import com.vmware.vim25.mo.*;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.service.Service;
import org.corfudb.universe.service.VMService;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.*;

import static lombok.Builder.Default;

/**
 * This implementation provides a Cluster servers each of which a VM machine.
 * Deploy is going to deploy a list of servers
 */
@Slf4j
@Builder
@AllArgsConstructor
public class VmCluster implements Cluster {

    @Getter
    private final VmClusterParams clusterParams;
    private final VirtualMachine appliance;
    @Default
    private final ImmutableList<Service> services = ImmutableList.of();

    @Override
    public VmCluster deploy() throws ClusterException {
        /**
         * setup parameters for appliances.
         * setup network?
         * start appliances
         * setup services
         * setup nodes
         * start nodes (jvm processes inside appliances). Don't need it if it's started during appliance deployment.
         */

        List<Service> servicesSnapshot = createAndDeployServices();

        return VmCluster.builder()
                .clusterParams(clusterParams)
                .appliance(appliance)
                .services(ImmutableList.copyOf(servicesSnapshot))
                .build();
    }

    @Override
    public void shutdown() throws ClusterException {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    public ImmutableList<Service> getServices() {
        return services;
    }

    private List<Service> createAndDeployServices() {
        List<Service> servicesSnapshot = new ArrayList<>();

        for (String serviceName : clusterParams.getServices().keySet()) {
            VMService service = VMService.builder()
                    .clusterParams(clusterParams)
                    .params(clusterParams.getService(serviceName))
                    .appliance(appliance)
                    .build();

            service = service.deploy();

            servicesSnapshot.add(service);
        }

        return servicesSnapshot;
    }

    @Builder
    @Getter
    public static class VmClusterParams extends ClusterParams {
        /*
         Specify appliances parameters here
         */
        String vSphereUrl = "https://10.173.65.98/sdk";
        String vSphereUsername  = "administrator@vsphere.local";
        String vSpherePassword = "XXXXXXXXXX";

        String templateVMName = "IntegTestVMTemplate";

        public static final Map<String, String> hostToIP;
        static
        {
            hostToIP = new HashMap<>();
            hostToIP.put("corfu-server-1", "10.173.65.101");
            hostToIP.put("corfu-server-2", "10.173.65.102");
            hostToIP.put("corfu-server-3", "10.173.65.103");
        }

        public static VirtualMachineCloneSpec createLinuxCustomization(String hostname, String ip_address){
            VirtualMachineCloneSpec vmCloneSpec = new VirtualMachineCloneSpec();

            //Set location of clone to be the same as template (Datastore)
            VirtualMachineRelocateSpec vmRelocateSpec = new VirtualMachineRelocateSpec();
            vmCloneSpec.setLocation(vmRelocateSpec);

            //Clone is not powered on, not a template.
            vmCloneSpec.setPowerOn(true);
            vmCloneSpec.setTemplate(false);

            //Create customization specs/linux specific options
            CustomizationSpec customSpec = new CustomizationSpec();
            CustomizationLinuxOptions linuxOptions = new CustomizationLinuxOptions();
            customSpec.setOptions(linuxOptions);

            CustomizationLinuxPrep linuxPrep = new CustomizationLinuxPrep();
            linuxPrep.setDomain("corfu.test");
            linuxPrep.setHwClockUTC(true);
            linuxPrep.setTimeZone("America/Los_Angeles");

            CustomizationFixedName fixedName = new CustomizationFixedName();
            fixedName.setName(hostname);
            linuxPrep.setHostName(fixedName);
            customSpec.setIdentity(linuxPrep);

            //Network related settings
            CustomizationGlobalIPSettings globalIPSettings = new CustomizationGlobalIPSettings();
            globalIPSettings.setDnsServerList(new String[]{"10.172.40.1", "10.172.40.2"});
            customSpec.setGlobalIPSettings(globalIPSettings);

            CustomizationFixedIp fixedIp = new CustomizationFixedIp();
            fixedIp.setIpAddress(ip_address);

            CustomizationIPSettings customizationIPSettings = new CustomizationIPSettings();
            customizationIPSettings.setIp(fixedIp);
            customizationIPSettings.setGateway(new String[]{"10.172.211.253"});
            customizationIPSettings.setSubnetMask("255.255.255.0");

            CustomizationAdapterMapping adapterMapping = new CustomizationAdapterMapping();
            adapterMapping.setAdapter(customizationIPSettings);

            CustomizationAdapterMapping[] adapterMappings = new CustomizationAdapterMapping[]{adapterMapping};
            customSpec.setNicSettingMap(adapterMappings);

            //Set all customization to clone specs
            vmCloneSpec.setCustomization(customSpec);
            return vmCloneSpec;
        }
    }
}
