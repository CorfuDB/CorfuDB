package org.corfudb.universe.cluster.vm;

import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.mo.*;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.NodeException;

import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.time.Duration;

import static org.corfudb.universe.cluster.vm.VmCluster.VmClusterParams;
import static org.corfudb.universe.cluster.vm.VmCluster.VmClusterParams.createLinuxCustomization;

/**
 * Implements a VM instance representing a CorfuServer.
 */
@Slf4j
@Builder
public class CorfuServerOnAppliance implements CorfuServer {

    @Getter
    private final ServerParams params;
    private final VirtualMachine appliance;
    private final VmClusterParams clusterParams;


    /**
     * Deploys a Corfu server / VM clone from template
     */
    @Override
    public CorfuServerOnAppliance deploy() throws NodeException {

        VirtualMachine appliance = deployAppliance();

        return CorfuServerOnAppliance.builder()
                .clusterParams(clusterParams)
                .params(params)
                .appliance(appliance)
                .build();

    }

    /**
     * Deploy and power on a VM appliance in vSphere
     *
     * @return VirtualMachine instance
     */
    private VirtualMachine deployAppliance() throws NodeException {
        ServiceInstance si = null;
        VirtualMachine appliance = null;
        try {
            //Connect to vSphere server using VIJAVA
            si = new ServiceInstance(new URL(clusterParams.getVSphereUrl()), clusterParams.getVSphereUsername(),
                    clusterParams.getVSpherePassword(), true);

            //Find the template machine in the inventory
            InventoryNavigator inventoryNavigator = new InventoryNavigator(si.getRootFolder());
            VirtualMachine vmTemplate = (VirtualMachine) inventoryNavigator.
                    searchManagedEntity("VirtualMachine", clusterParams.getTemplateVMName());

            String host = params.getHost();
            String ipAddress = clusterParams.hostToIP.get(host);

            log.info("Deploying the Corfu server {} on {}", host, ipAddress);
            // Create customization for cloning process
            VirtualMachineCloneSpec cloneSpec = createLinuxCustomization(host, ipAddress);
            try {
                //Do the cloning - providing the clone specification
                Task cloneTask = vmTemplate.cloneVM_Task((Folder) vmTemplate.getParent(), host, cloneSpec);
                cloneTask.waitForTask();
            } catch (RemoteException | InterruptedException e) {
                e.printStackTrace();
            }
            // After the clone task completes, get the VM from the inventory
            appliance = (VirtualMachine) inventoryNavigator.searchManagedEntity("VirtualMachine", host);

        } catch (RemoteException | MalformedURLException e) {
            e.printStackTrace();
        }
        return appliance;
    }

    /**
     * This method attempts to gracefully stops the Corfu server. After timeout, it will kill the Corfu server.
     *
     * @param timeout a duration after which the stop will kill the server
     * @throws NodeException this exception will be thrown if the server can not be stopped.
     */
    @Override
    public void stop(Duration timeout) throws NodeException {
        String host = params.getHost();
        String ipAddress = clusterParams.hostToIP.get(host);
        log.info("Stopping the Corfu server {} on {}", host, ipAddress);
        // TODO on VM

    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() throws NodeException {
        String host = params.getHost();
        String ipAddress = clusterParams.hostToIP.get(host);
        log.info("Killing the Corfu server {} on {}", host, ipAddress);
        // TODO on VM

    }


    private String getAppVersion() throws IOException, XmlPullParserException {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(new FileReader("pom.xml"));
        return model.getParent().getVersion();
    }

    /**
     * This method create a command line string for starting Corfu server
     *
     * @return command line parameters
     */
    private String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append("0.0.0.0");

        switch (params.getPersistence()){
            case DISK:
                if(StringUtils.isEmpty(params.getLogDir())){
                    throw new ClusterException("Invalid log dir in disk persistence mode");
                }
                cmd.append(" -l ").append(params.getLogDir());
                break;
            case MEMORY:
                cmd.append(" -m");
                break;
        }

        if (params.getMode() == Mode.SINGLE) {
            cmd.append(" -s");
        }

        cmd.append(" -d ").append(params.getLogLevel().toString()).append(" ");

        cmd.append(params.getPort());

        String cmdLineParams = cmd.toString();
        log.trace("Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }
}
