package org.corfudb.utils.lock;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@Builder
public class LockConfig {
    @Default
    private final String lockGroup = "Log_Replication_Group";
    @Default
    private final String lockName = "Log_Replication_Lock";
    @Default
    private final int lockLeaseDurationInSeconds = 300;
    @Default
    private final int lockMonitorDurationInSeconds = 60;
    @Default
    private final int lockDurationBetweenLeaseChecksSeconds = 60;
    @Default
    private final int lockDurationBetweenLeaseRenewalsSeconds = 60;
    @Default
    private final int lockMaxTimeListenerNotificationSeconds = 60;
}
