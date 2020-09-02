package org.corfudb.infrastructure.log.store;

import static com.google.common.base.Preconditions.checkState;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;
import org.lmdbjava.ByteBufferProxy;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;

public class Database {
  private final Env<ByteBuffer> env;

  private final Dbi<ByteBuffer> database;

  private final PooledByteBufAllocator allocator;

  private final boolean flush;

  public Database(
      final Path path,
      final long maxSize,
      final PooledByteBufAllocator allocator,
      final boolean flush) {

    path.toFile().mkdir();

    this.allocator = allocator;
    this.flush = flush;
    env = create(ByteBufferProxy.PROXY_OPTIMAL).setMapSize(maxSize).setMaxDbs(1).open(path.toFile(), MDB_NOSYNC);
    database = env.openDbi("database", MDB_CREATE);
  }

  /** Flushes buffers to secondary storage. // TODO(Maithem): test failure scenarios? */
  public void sync() {
    if (flush) {
      env.sync(true);
    }
  }

  /**
   * Atomically write multiple payloads. The sequence must map one to one with the payalods. For
   * example, sequences[0] must correspond to payloads[0] etc...)
   */
  public void append(final Set<LogData> batch) {
    if (batch.isEmpty()) {
      return;
    }
    // TODO(Maithem) check duplicates and overwrites

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      for (LogData ld : batch) {
        checkState(Address.isAddress(ld.getGlobalAddress()));
        final ByteBuf key = allocator.buffer(Long.BYTES);
        final ByteBuf value = allocator.buffer();
        try (final ILogData.SerializationHandle handle = ld.getSerializedForm(true)) {
          final LogData sh = (LogData) handle.getSerialized();
          key.writeLong(ld.getGlobalAddress());
          sh.doSerialize(value);
          //checkState(database.put(txn, key, value.asReadOnly()));
          checkState(database.put(txn, key.nioBuffer(key.readerIndex(), key.writerIndex()),
                  value.nioBuffer(value.readerIndex(), value.writerIndex())));
          //System.out.println("address " + ld.getGlobalAddress());
        } finally {
          key.release();
          value.release();
        }
      }

      txn.commit();
      // TODO(Maithem): Sync here and disable external sync requests ??
    }
  }

  public LogData read(final long address) {
    checkState(Address.isAddress(address));
    return read(Collections.singleton(address)).get(address);
  }

  /**
   * Read multiple sequences within the same snapshot.
   *
   * @return an array of payloads that corresponds to the sequences, or null if they key doesn't
   *     have a mapping
   */
  public Map<Long, LogData> read(final Set<Long> addresses) {

    if (addresses.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<Long, LogData> result = new HashMap<>(addresses.size());
    final ByteBuf key = allocator.buffer(Long.BYTES);
    try (Txn<ByteBuffer> txn = env.txnRead()) {

      for (long address : addresses) {
        key.clear();
        checkState(Address.isAddress(address));
        key.writeLong(address);
        ByteBuffer payload = database.get(txn, key.nioBuffer(key.readerIndex(), key.writerIndex()));

        if (payload == null) {
          result.put(address, null);
          continue;
        }

        final LogData ld = new LogData(Unpooled.wrappedBuffer(payload));
        checkState(address == ld.getGlobalAddress());
        result.put(address, ld);
        // payload.release();
      }

      txn.commit();
      return result;
    } finally{
      key.release();
    }
  }

  public void reverseIter(Consumer<LogData> ldConsumer, long stop) {
    Objects.requireNonNull(ldConsumer);

    try (Txn<ByteBuffer> txn = env.txnRead();
        Cursor<ByteBuffer> cursor = database.openCursor(txn)) {

      if (!cursor.seek(SeekOp.MDB_LAST)) {
        return;
      }

      do {
        ByteBuffer key = cursor.key();
        long address = key.getLong();
        if (address < stop) {
          break;
        }

        ByteBuffer val = cursor.val();
        Objects.requireNonNull(val);
        final LogData ld = new LogData(Unpooled.wrappedBuffer(val));
        ldConsumer.accept(ld);
        //System.out.println(ld);
      } while (cursor.prev());

      txn.commit();
    }
  }

  public int trim(final long trimPoint) {
    int totalDeleted = 0;
    int deleted;
    do {
      deleted = trim0(trimPoint, 1000);
      totalDeleted += deleted;
    } while (deleted != 0);

    return totalDeleted;
  }

  private int trim0(final long trimPoint, final int maxBatchSize) {
    int numDeleted = 0;
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (Cursor<ByteBuffer> cursor = database.openCursor(txn)) {

        if (!cursor.seek(SeekOp.MDB_FIRST)) {
          // duplicate entries allowed?
          // does fitry entry mean smallest key ?
          // System.out.println("Couldn't find first entry");
          // txn.abort();
          return 0;
        }

        do {
          final ByteBuffer key = cursor.key();
          // TODO(Maithem): does this allocate buff?
          final long currentSequence = key.getLong();
          if (currentSequence >= trimPoint) {
            break;
          }

          key.position(0);
          numDeleted++;
          checkState(database.delete(txn, key), " cursor corrupted");
        } while (cursor.next() && numDeleted < maxBatchSize);
      }
      txn.commit();
      // sync();
    }
    return numDeleted;
  }

  public void close() {
      sync();
    database.close();
    env.close();
  }
}
