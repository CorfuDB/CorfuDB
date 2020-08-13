package org.corfudb.utils.lock;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

@Getter
@AllArgsConstructor
@ToString
@Builder
@Slf4j
public class LockConfig {
    @Default
    private final String lockGroup = "Log_Replication_Group";
    @Default
    private final String lockName = "Log_Replication_Lock";
    @Default
    private final int lockLeaseDurationInSeconds = 60;
    @Default
    private final int lockMonitorDurationInSeconds = 6;
    @Default
    private final int lockDurationBetweenLeaseChecksSeconds = 6;
    @Default
    private final int lockDurationBetweenLeaseRenewalsSeconds = 15;
    @Default
    private final int lockDurationBetweenNotificationCallbacksSeconds = 60;

    /**
     * Return a new instance of a LockConfig given an optional resource file path.
     * If the resource file is not present, not found, or there were issues parsing a config file -
     * create a default instance of a LockConfig using it's builder.
     * @param maybeConfigFile An optional path to the resource config file
     * @return A new instance of a lock config
     */
    public static LockConfig newInstance(Optional<String> maybeConfigFile) {
        if (!maybeConfigFile.isPresent()) {
            log.warn("No config file provided. Creating default lock config.");
            return LockConfig.builder().build();
        }
        String configFile = maybeConfigFile.get();
        log.info("Configuring lock with a file: {}", configFile);
        Optional<URL> url;
        try {
            Enumeration<URL> systemResources = ClassLoader.getSystemResources(configFile);
            if (systemResources.hasMoreElements()) {
                url = Optional.of(systemResources.nextElement());
            }
            else {
                throw new FileNotFoundException(configFile);
            }
        }
        catch (IOException e) {
            LockConfig config = LockConfig.builder().build();
            log.warn("Error occurred while trying to read a lock config resource {}. " +
                    "Using default lock config: {}.", configFile, config, e);
            return config;
        }
        try (InputStream input = url.get().openStream()) {
            Properties prop = new Properties();
            prop.load(input);
            String lockGroup = prop.getProperty("lock_group");
            String lockName = prop.getProperty("lock_name");
            int lockLeaseDurationSeconds = Integer.parseInt(prop.getProperty("lock_lease_duration_seconds"));
            int lockMonitorDurationSeconds = Integer.parseInt(prop.getProperty("lock_monitor_duration_seconds"));
            int lockDurationBetweenLeaseChecksSeconds = Integer.parseInt(prop.getProperty("lock_duration_between_lease_checks_seconds"));
            int lockDurationBetweenLeaseRenewalsSeconds = Integer.parseInt(prop.getProperty("lock_duration_between_lease_renewals_seconds"));
            int lockDurationBetweenNotificationCallbacksSeconds = Integer.parseInt(prop.getProperty("lock_duration_between_notification_callbacks_seconds"));
            LockConfig config = LockConfig.builder()
                    .lockGroup(lockGroup)
                    .lockName(lockName)
                    .lockLeaseDurationInSeconds(lockLeaseDurationSeconds)
                    .lockMonitorDurationInSeconds(lockMonitorDurationSeconds)
                    .lockDurationBetweenLeaseChecksSeconds(lockDurationBetweenLeaseChecksSeconds)
                    .lockDurationBetweenLeaseRenewalsSeconds(lockDurationBetweenLeaseRenewalsSeconds)
                    .lockDurationBetweenNotificationCallbacksSeconds(lockDurationBetweenNotificationCallbacksSeconds)
                    .build();
            log.info("Using lock config: {}", config);
            return config;
        } catch (Exception e) {
            LockConfig config = LockConfig.builder().build();
            log.warn("Error occurred parsing the lock config {}. " +
                    "Using default lock config: {}.", configFile, config, e);
            return config;
        }
    }
}
