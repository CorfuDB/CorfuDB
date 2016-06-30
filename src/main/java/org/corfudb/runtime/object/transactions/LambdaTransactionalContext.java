package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.ICorfuSMRObject;
import org.corfudb.runtime.view.ObjectOpenOptions;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class LambdaTransactionalContext extends AbstractTransactionalContext {

    @Getter
    final long firstReadTimestamp;

    class LambdaTransactionalObjectData<P> {

        CorfuSMRObjectProxy<P> originalProxy;
        CorfuSMRObjectProxy<P> cloneProxy;

        public LambdaTransactionalObjectData(CorfuSMRObjectProxy<P> proxy)
        {
            this.originalProxy = proxy;
            syncAndLockProxy();
        }

        public CorfuSMRObjectProxy<P> getProxy() {
            if (cloneProxy != null) return cloneProxy;
            return originalProxy;
        }

        public void releaseLock() {
            if (firstReadTimestamp > originalProxy.getTimestamp()) {
                originalProxy.setTimestamp(firstReadTimestamp);
            }
            if (firstReadTimestamp > originalProxy.getSv().getLogPointer()) {
                originalProxy.getSv().setLogPointer(firstReadTimestamp);
            }
            if (cloneProxy == null) originalProxy.getRwLock().writeLock().unlock();
        }

        public void cloneProxy() {
            // the proxied object is ahead of the lambda tx. we must create a new proxy...
            log.debug("Proxied object at {}, lambda tx at {}, creating clone to service TXN",
                    originalProxy.getTimestamp(), firstReadTimestamp);
            ICorfuSMRObject<P> obj = (ICorfuSMRObject<P>)
                    runtime.getObjectsView().build()
                            .setOptions(EnumSet.of(ObjectOpenOptions.NO_CACHE))
                            .setStreamID(originalProxy.getStreamID())
                            .setType(originalProxy.getOriginalClass())
                            .setArgumentsArray(originalProxy.getCreationArguments())
                            .open();
            cloneProxy = (CorfuSMRObjectProxy<P>) obj.getProxy();
            cloneProxy.sync(obj.getSMRObject(), firstReadTimestamp - 1L);
            originalProxy.getRwLock().writeLock().unlock();
        }

        @SuppressWarnings("unchecked")
        public void syncAndLockProxy() {
          //  if (!originalProxy.getRwLock().writeLock().tryLock()) {
           //     log.info("lock fail");
           //     cloneProxy();
           // }
          //  else {
            originalProxy.getRwLock().writeLock().lock();
                log.info("proxy locked {}", originalProxy.getStreamID());
                if (firstReadTimestamp < originalProxy.getTimestamp()) {
                    cloneProxy();
                    originalProxy.getRwLock().writeLock().unlock();
                }
                // Lock is re-entrant, safe to call sync
                else {
                    log.debug("Sync object to {}", firstReadTimestamp - 1L);
                    originalProxy.sync(originalProxy.getSmrObject(), firstReadTimestamp - 1L);
                }
           // }
        }
    }

    public Map<UUID, LambdaTransactionalObjectData> objectDataMap;

    public LambdaTransactionalContext(CorfuRuntime runtime, long lambdaTimestamp) {
        super(runtime);
        firstReadTimestamp = lambdaTimestamp;
        objectDataMap = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public <P> LambdaTransactionalObjectData<P> getObjectData(CorfuSMRObjectProxy<P> proxy) {
        return objectDataMap.computeIfAbsent(proxy.getStreamID(), x -> new LambdaTransactionalObjectData(proxy));
    }

    /**
     * Check if there was nothing to write.
     *
     * @return Return true, if there was no write set.
     */
    @Override
    public boolean hasNoWriteSet() {
        return false;
    }

    @Override
    public <T> void resetObject(CorfuSMRObjectProxy<T> proxy) {

    }

    @Override
    public void addTransaction(AbstractTransactionalContext tc) {

    }

    /**
     * Add to the read set
     *
     * @param proxy     The SMR Object proxy to get an object for writing.
     * @param SMRMethod
     * @param result    @return          An object for writing.
     */
    @Override
    public <T> void addReadSet(CorfuSMRObjectProxy<T> proxy, String SMRMethod, Object result) {

    }

    /**
     * Open an object for reading. The implementation will avoid creating a copy of the object
     * if it has not already been done.
     *
     * @param proxy The SMR Object proxy to get an object for reading.
     * @return An object for reading.
     */
    @Override
    public <T> T getObjectRead(CorfuSMRObjectProxy<T> proxy) {
        LambdaTransactionalObjectData<T> objData = getObjectData(proxy);
        return objData.getProxy().getSmrObject();
    }

    /**
     * Open an object for writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @return An object for writing.
     */
    @Override
    public <T> T getObjectWrite(CorfuSMRObjectProxy<T> proxy) {
        LambdaTransactionalObjectData<T> objData = getObjectData(proxy);
        return objData.getProxy().getSmrObject();
    }

    /**
     * Open an object for reading and writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @return An object for writing.
     */
    @Override
    public <T> T getObjectReadWrite(CorfuSMRObjectProxy<T> proxy) {
        LambdaTransactionalObjectData<T> objData = getObjectData(proxy);
        return objData.getProxy().getSmrObject();
    }

    @Override
    public void close() {
        super.close();
        objectDataMap.values().stream()
                .forEach(LambdaTransactionalObjectData::releaseLock);
    }
}
