package org.corfudb.infrastructure.management;

import javax.management.MBeanServer;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;

/**
 * https://stackoverflow.com/questions/28495280/how-to-detect-a-long-gc-from-within-a-jvm
 *
 */
public class SystemMonitoring implements MonitoringService {

    @Override
    public void start(Duration monitoringInterval) {
        //free memory, free disk, GC pauses,

    }

    @Override
    public void shutdown() {

    }
}
