package org.corfudb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class FileWatcher implements Closeable {

    private final File file;

    private String lastFileHash;

    private final Runnable onChange;

    private volatile WatchService watchService;

    private ExecutorService executorService;

    @Getter
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @Getter
    private final AtomicBoolean isRegistered = new AtomicBoolean(false);

    public FileWatcher(String filePath, Runnable onChange, ExecutorService executorService){
        this.file = Paths.get(filePath).toFile();
        this.onChange = onChange;
        this.executorService = executorService;
        executorService.submit(this::start);
    }

    public FileWatcher(String filePath, Runnable onChange){
        this(filePath, onChange, newExecutorService());
    }

    private static ExecutorService newExecutorService() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("FileWatcher-%d")
                .build();

        return Executors.newSingleThreadExecutor(threadFactory);
    }

    private void start() {
        while (!isStopped.get()) {
            LambdaUtils.runSansThrow(this::poll);
        }
    }

    private void poll() {
        try {
            if (!isRegistered.get()) {
                log.warn("FileWatcher doesn't have directory registered inside the poll cycle!");
                reloadNewWatchService();
            }

            // Blocked until a key is returned
            WatchKey key = watchService.take();
            if (key == null) {
                return;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();

                log.info("FileWatcher: event kind: {}, event filename: {}, watched file: {}",
                        kind.toString(), filename.toString(), file.getName());

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    log.warn("FileWatcher hit overflow and events might be lost!");
                } else if ((kind == StandardWatchEventKinds.ENTRY_MODIFY ||
                        kind == StandardWatchEventKinds.ENTRY_CREATE ||
                        kind == StandardWatchEventKinds.ENTRY_DELETE)
                        && filename.toString().equals(file.getName())) {

                    String curFileHash = getFileHash();
                    if (!Objects.equals(curFileHash, this.lastFileHash)) {
                        this.lastFileHash = curFileHash;
                        log.info("FileWatcher: hashcode of file {} changed. Invoking handler...", filename);
                        onChange.run();
                    } else {
                        log.info("FileWatcher: hashcode of file {} has NOT changed. Skipping handler...", filename);
                    }
                }
            }
            // reset key for continuous watching
            key.reset();
        } catch (Throwable t) {
            // Check if the FileWatcher is stopped and log accordingly
            // to avoid throwing unintentional ERROR statements
            if (isStopped.get()) {
                log.info("FileWatcher failed to poll file {}, Exception: {}., isStopped: {}",
                        file.getAbsoluteFile(), t, isStopped.get());
            } else {
                log.error("FileWatcher failed to poll file {}", file.getAbsoluteFile(), t);
            }
            reloadNewWatchService();
        }
    }

    private String getFileHash() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] fileBytes = Files.readAllBytes(this.file.toPath());
            byte[] hashBytes = digest.digest(fileBytes);
            return Base64.getEncoder().encodeToString(hashBytes); // Base64 for compact storage
        } catch (IOException | NoSuchAlgorithmException e) {
            log.warn("Failed to compute file hash: {}", this.file.toPath(), e);
            return "";
        }
    }

    private void reloadNewWatchService() {
        isRegistered.set(false);
        if (isStopped.get()) {
            log.info("Watch service is stopped. Skip reloading new watch service.");
            return;
        }

        try {
            if (watchService != null) {
                watchService.close();
            }
            watchService = FileSystems.getDefault().newWatchService();
            Path path = file.toPath().getParent();
            this.lastFileHash = getFileHash();
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
            isRegistered.set(true);
            log.info("FileWatcher: parent dir {} for file {} registered.", path, file.getAbsoluteFile());
        } catch (IOException ioe) {
            throw new IllegalStateException("Failed to start a new watch service!", ioe);
        }
    }

    @Override
    public void close() {
        isStopped.set(true);

        try {
            if (watchService != null) {
                watchService.close();
            }
            isRegistered.set(false);
        } catch (IOException ioe) {
            throw new IllegalStateException("FileWatcher failed to close the watch service!", ioe);
        }
        this.executorService.shutdownNow();
        log.info("Closed FileWatcher.");
    }

    /**
     * Create and get an optional of FileWatcher on keyStorePath file
     *
     * @param filePath The file path to register the File Watcher on.
     * @param onChange Runnable callback method to call after a change is detected by the watcher.
     *
     * @return The Optional of FileWatcher on the filePath. Empty if the filePath is not set.
     */
    public static Optional<FileWatcher> newInstance(
            String filePath,
            Runnable onChange
    ) {
        if (filePath == null || filePath.isEmpty()) {
            return Optional.empty();
        }
        FileWatcher sslWatcher = new FileWatcher(filePath, onChange);
        return Optional.of(sslWatcher);
    }
}
