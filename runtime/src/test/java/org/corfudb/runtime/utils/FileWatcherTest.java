package org.corfudb.runtime.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.util.FileWatcher;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test {@link FileWatcher}'s life cycle and functionality.
 * Created by cgudisagar on 2/17/24.
 */
@Slf4j
public class FileWatcherTest {

    private static FileWatcher fileWatcher;
    private static String PATH;
    private Path filePath;
    private ExecutorService executorService;
    private static final int SLEEP_TIMER_1S = 1;
    private static final int MAX_RETRY_LIMIT = 20;

    @Before
    public void setup() throws IOException {
        PATH = com.google.common.io.Files.createTempDir().getAbsolutePath();
        filePath = Paths.get(PATH , "keystore.jks");
        Files.createFile(filePath);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(PATH));
        fileWatcher.close();
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    /**
     * Test that executor service of the file watcher service
     * is started and shutdown correctly
     */
    @Test
    public void testFileWatcherExecutorService() {
        executorService = mock(ExecutorService.class);
        fileWatcher = spy(new FileWatcher(filePath.toFile().getAbsolutePath(), () -> {}, executorService));
        verify(executorService, times(1)).submit(any(Runnable.class));
        fileWatcher.close();
        verify(fileWatcher, times(1)).close();
        verify(executorService, times(1)).shutdownNow();
    }

    /**
     * Test that the file watcher callback is invoked correctly when the file content is modified,
     * and not invoked when the file permissions are changed.
     */
    @Test
    public void testFileWatcherLastModified() throws IOException, InterruptedException {
        // NIO WatchService does not work properly on macOS
        // To run this IT on macOS, one can configure in IntelliJ Idea to run remotely on docker containers
        Assume.assumeTrue(System.getProperty("os.name").contains("Linux"));

        AtomicInteger onChangeCounter = new AtomicInteger(0);
        fileWatcher = new FileWatcher(filePath.toFile().getAbsolutePath(), onChangeCounter::incrementAndGet);

        // FileWatcher 'executorService.submit(this::start);' is async
        TimeUnit.SECONDS.sleep(1);

        byte[] randomBytes = new byte[100];
        Random random = new Random();
        random.nextBytes(randomBytes);
        Files.write(filePath.toAbsolutePath(), randomBytes,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

        // Verify that the file watch callback has been invoked
        TimeUnit.SECONDS.sleep(1);
        assertThat(onChangeCounter.get()).isEqualTo(1);

        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxrwx---");
        Files.setPosixFilePermissions(filePath.toAbsolutePath(), permissions);

        // Verify that the file watch callback has NOT been invoked
        TimeUnit.SECONDS.sleep(1);
        assertThat(onChangeCounter.get()).isEqualTo(1);
    }

    /**
     * Test that {@link FileWatcher}'s OnChance is triggered correctly when the watched file
     * contents are changed.
     * Test that on close method closes the executor service correctly and sets the statuses of
     * {@link FileWatcher#getIsRegistered()} and {@link FileWatcher#getIsStopped()}.
     *
     * @throws InterruptedException When a sleeping thread is interrupted.
     * @throws IOException Any File IO Exception
     */
    @Test
    public void testFileWatcherOnChangeAndClose() throws InterruptedException, IOException {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("FileWatcher")
                .build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);
        AtomicInteger onChangeCounter = new AtomicInteger(0);
        Runnable onChange = () -> {
            log.info("onChangeCounter - " + onChangeCounter.get() + " lastModified " +
            filePath.toFile().lastModified() + " size " + filePath.toFile().length() );
            onChangeCounter.incrementAndGet();
            log.info("onChangeCounter - " + onChangeCounter.get());
        };

        fileWatcher = spy(new FileWatcher(filePath.toFile().getAbsolutePath(), onChange, executorService));

        assertThat(onChangeCounter.get()).isZero();

        for (int i = 0; i < MAX_RETRY_LIMIT; i+=1) {
            log.info("iteration: " + i + " onChangeCounter: " + onChangeCounter.get() + " lastModified: " +
                    filePath.toFile().lastModified() + " size: " + filePath.toFile().length());
            if (fileWatcher.getIsRegistered().get()) {
                log.info("fileWatcher.getIsRegistered: " + fileWatcher.getIsRegistered().get());
                break;
            }
        }

        long lastModifiedTime = filePath.toFile().lastModified();

        Path keyStoreFilePathTemp = filePath.resolveSibling(filePath.getFileName() + ".temp");
        byte[] randomBytes = new byte[100];
        Random random = new Random();
        random.nextBytes(randomBytes);

        Files.write(keyStoreFilePathTemp, randomBytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        Files.move(keyStoreFilePathTemp, filePath, StandardCopyOption.ATOMIC_MOVE);

        // Avoid same lastModifiedTime for the new file
        if (filePath.toFile().lastModified() == lastModifiedTime) {
            log.info("Writing to file again to avoid same lastModifiedTime. lastModifiedTime: {}", lastModifiedTime);
            TimeUnit.SECONDS.sleep(SLEEP_TIMER_1S);
            Files.write(keyStoreFilePathTemp, randomBytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            Files.move(keyStoreFilePathTemp, filePath, StandardCopyOption.ATOMIC_MOVE);
        }

        for (int i = 0; i < MAX_RETRY_LIMIT; i+=1) {
            log.info("iteration: " + i + " onChangeCounter: " + onChangeCounter.get() + " lastModified: " +
                    filePath.toFile().lastModified() + " size: " + filePath.toFile().length());
            if (onChangeCounter.get() == 1) {
                break;
            }
            TimeUnit.SECONDS.sleep(SLEEP_TIMER_1S);
        }
        assertThat(onChangeCounter.get()).isEqualTo(1);

        assertThat(executorService.isShutdown()).isFalse();
        assertThat(fileWatcher.getIsStopped()).isFalse();
        assertThat(fileWatcher.getIsRegistered()).isTrue();

        fileWatcher.close();

        verify(fileWatcher, times(1)).close();
        assertThat(executorService.isShutdown()).isTrue();
        assertThat(fileWatcher.getIsStopped()).isTrue();
        assertThat(fileWatcher.getIsRegistered()).isFalse();
    }
}
