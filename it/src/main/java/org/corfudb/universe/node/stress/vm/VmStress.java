package org.corfudb.universe.node.stress.vm;

import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.cluster.vm.RemoteOperationHelper;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.stress.Stress;
import org.corfudb.universe.universe.vm.VmUniverseParams;

@Slf4j
@Builder
public class VmStress implements Stress {
    @NonNull
    private final CorfuServerParams params;
    @NonNull
    private final VirtualMachine vm;
    private final RemoteOperationHelper commandHelper = RemoteOperationHelper.getInstance();
    @NonNull
    private final VmUniverseParams universeParams;

    /**
     * To stress CPU usage on {@link CorfuServer} node.
     */
    @Override
    public void stressCPULoad() {
        log.info("Stressing CPU on corfu server: {}", params.getName());

        String cmd = "stress -c " + vm.getSummary().getConfig().getNumCpu();
        executeOnVm(cmd);
    }

    /**
     * To stress IO usage on {@link CorfuServer} node.
     */
    @Override
    public void stressIOLoad() {
        log.info("Stressing I/O on corfu server: {}", params.getName());

        String cmd = "stress -i " + vm.getSummary().getConfig().getNumCpu();
        executeOnVm(cmd);
    }

    /**
     * To stress memory (RAM) usage on {@link CorfuServer} node.
     */
    @Override
    public void stressMemoryLoad() {
        log.info("Stressing Memory (RAM) on corfu server: {}", params.getName());

        String cmd = "stress -m " + vm.getSummary().getConfig().getNumCpu() + " --vm-bytes 1G";
        executeOnVm(cmd);
    }

    /**
     * To stress disk usage on {@link CorfuServer} node.
     */
    @Override
    public void stressDiskLoad() {
        log.info("Stressing disk on corfu server: {}", params.getName());

        String cmd = "stress -d " + vm.getSummary().getConfig().getNumCpu() + " --hdd-bytes 5G";
        executeOnVm(cmd);
    }

    /**
     * To release the existing stress load on {@link CorfuServer} node.
     */
    @Override
    public void releaseStress() {
        log.info("Release the stress load on corfu server: {}", params.getName());

        executeOnVm("ps -ef | grep -v grep | grep \"stress\" | awk '{print $2}' | xargs kill -9");
    }

    /**
     * Executes a certain command on the VM.
     */
    private void executeOnVm(String cmdLine) {
        String ipAddress = getIpAddress();

        commandHelper.executeCommand(ipAddress,
                universeParams.getVmUserName(),
                universeParams.getVmPassword(),
                cmdLine
        );
    }

    /**
     * @return the IpAddress of this VM.
     */
    private String getIpAddress() {
        GuestInfo guest = vm.getGuest();
        return guest.getIpAddress();
    }
}
