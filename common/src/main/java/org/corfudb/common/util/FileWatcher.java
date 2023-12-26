package org.corfudb.common.util;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FileWatcher implements Closeable {

    private final File file;

    private final Runnable onChange;

    private final ScheduledExecutorService watcher;

    private volatile WatchService watchService;


    public FileWatcher(String filePath, Runnable onChange, Duration pollPeriod){
        this.file = Paths.get(filePath).toFile();
        this.onChange = onChange;

        this.watcher = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("FileWatcher-")
                        .build());

        reloadNewWatchService();
        watcher.scheduleAtFixedRate(
                this::poll,
                pollPeriod.toMillis(),
                pollPeriod.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void poll() {
        try {
            WatchKey key = watchService.poll();
            if (key == null) {
                return;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    log.warn("FileWatcher hit overflow and events might be lost!");
                } else if (kind == java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
                        && filename.toString().equals(file.getName())) {
                    log.info("FileWatcher: file {} changed. Invoking handler...", filename);
                    onChange.run();
                }
            }
            // reset key for continuous watching
            key.reset();
        } catch (Throwable t) {
            log.error("FileWatcher failed to poll file {}", file.getAbsoluteFile(), t);
            reloadNewWatchService();
        }
    }

    private void reloadNewWatchService() {
        try {
            if (watchService != null) {
                watchService.close();
            }
            watchService = FileSystems.getDefault().newWatchService();
            Path path = file.toPath().getParent();
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            log.info("FileWatcher: parent dir {} for file {} registered.", path, file.getAbsoluteFile());
        } catch (IOException ioe) {
            throw new IllegalStateException("Failed to start a new watch service!", ioe);
        }
    }

    @Override
    public void close() {
        this.watcher.shutdownNow();
        try {
            this.watchService.close();
        } catch (IOException ioe) {
            throw new IllegalStateException("FileWatcher failed to close the watch service!.", ioe);
        }
    }
}
