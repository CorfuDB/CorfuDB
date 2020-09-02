package org.corfudb.infrastructure.log;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;

/** Created by maithem on 11/2/16. */
public class StreamLogFilesTest extends AbstractCorfuTest {

  private String getDirPath() {
    return PARAMETERS.TEST_TEMP_DIR;
  }

  private ServerContext getContext() {
    String path = getDirPath();
    return new ServerContextBuilder().setLogPath(path).setMemory(false).build();
  }
/**
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void reverseTest() {
        final Env<ByteBuf> env;
        final Dbi<ByteBuf> database;
        env =
                create(ByteBufProxy.PROXY_NETTY)
                        .setMapSize(1024L * 1024L)
                        .setMaxDbs(1)
                        .open(Paths.get(getDirPath()).toFile(), MDB_NOSYNC);

        database = env.openDbi("database", MDB_CREATE);

        final long first = 0L;
        final long second = 100L;
        final long third = 10000000L;

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(100);
        byte[] data = new byte[100];
        payload.writeBytes(data);

        ByteBuf key1 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        ByteBuf key2 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        ByteBuf key3 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);

        key1.writeLong(first);
        key2.writeLong(second);
        key3.writeLong(third);

        database.put(key1, payload);
        payload.resetReaderIndex();

        database.put(key2, payload);
        payload.resetReaderIndex();

        database.put(key3, payload);

        key1.resetReaderIndex();
        key2.resetReaderIndex();
        key3.resetReaderIndex();

        try (Txn<ByteBuf> txn = env.txnRead()) {

            assert database.get(txn, key1) != null;
            assert database.get(txn, key2) != null;
            assert database.get(txn, key3) != null;

            txn.commit();
        }

        int numKeys = 0;

        ByteBuf end = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        end.writeLong(Long.MAX_VALUE);

        try (Txn<ByteBuf> txn = env.txnRead();
             CursorIterable<ByteBuf> c = database.iterate(txn, KeyRange.closedOpen(key1, end))) {
            for (final CursorIterable.KeyVal<ByteBuf> kv : c) {
                assert kv.key() != null;
                assert kv.val() != null;
                numKeys++;
            }
            txn.commit();
        }

        assert numKeys == 3;
    }

        @Test
        public void reverseTest34() {
            final Env<ByteBuf> env;
            final Dbi<ByteBuf> database;
            env =
                    create(ByteBufProxy.PROXY_NETTY)
                            .setMapSize(1024L * 1024L)
                            .setMaxDbs(1)
                            .open(Paths.get(getDirPath()).toFile(), MDB_NOSYNC);

            database = env.openDbi("database", MDB_CREATE);

            final long first = 0L;
            final long second = 100L;
            final long third = 10000000L;

            ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(100);
            byte[] data = new byte[100];
            payload.writeBytes(data);

            ByteBuf key1 = PooledByteBufAllocator.DEFAULT.directBuffer(Long.BYTES);
            ByteBuf key2 = PooledByteBufAllocator.DEFAULT.directBuffer(Long.BYTES);
            ByteBuf key3 = PooledByteBufAllocator.DEFAULT.directBuffer(Long.BYTES);

            key1.writeLong(first);

            key2.writeLong(second);

            key3.writeLong(third);

            database.put(key1, payload);
            payload.resetReaderIndex();

            database.put(key2, payload);
            payload.resetReaderIndex();

            database.put(key3, payload);

            key1.resetReaderIndex();
            key2.resetReaderIndex();
            key3.resetReaderIndex();

            try (Txn<ByteBuf> txn = env.txnRead()) {

                assert database.get(txn, key1) != null;
                assert database.get(txn, key2) != null;
                assert database.get(txn, key3) != null;

                txn.commit();
            }

            final int[] numKeys = new int[1];

            ByteBuf end = PooledByteBufAllocator.DEFAULT.directBuffer(Long.BYTES);
            end.writeLong(Long.MAX_VALUE);

            key1.resetReaderIndex();

            try (Txn<ByteBuf> txn = env.txnRead();
                 CursorIterable<ByteBuf> c = database.iterate(txn, KeyRange.closedOpenBackward(key1, end))) {
                Iterator<CursorIterable.KeyVal<ByteBuf>> iter = c.iterator();

                iter.forEachRemaining(kv -> {
                    assert kv.key() != null;
                    assert kv.val() != null;
                    numKeys[0]++;
                });


                txn.commit();
            }

            assert numKeys[0] == 3;
        }



    //@Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void reverseTest2() {
        final Env<ByteBuf> env;
        final Dbi<ByteBuf> database;
        env =
                create(ByteBufProxy.PROXY_NETTY)
                        .setMapSize(1024L * 1024L)
                        .setMaxDbs(1)
                        .open(Paths.get(getDirPath()).toFile(), MDB_NOSYNC);

        database = env.openDbi("database", MDB_CREATE);

        final long first = 0L;
        final long second = 100L;
        final long third = 10000000L;

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(100);
        byte[] data = new byte[100];
        payload.writeBytes(data);

        ByteBuf key1 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        ByteBuf key2 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        ByteBuf key3 = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);

        key1.writeLong(first);
        key2.writeLong(second);
        key3.writeLong(third);

        database.put(key1, payload);
        payload.resetReaderIndex();

        database.put(key2, payload);
        payload.resetReaderIndex();

        database.put(key3, payload);

        key1.resetReaderIndex();
        key2.resetReaderIndex();
        key3.resetReaderIndex();

        try (Txn<ByteBuf> txn = env.txnRead()) {

            assert database.get(txn, key1) != null;
            assert database.get(txn, key2) != null;
            assert database.get(txn, key3) != null;

            txn.commit();
        }

        int numKeys = 0;

        ByteBuf end = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
        end.writeLong(Long.MAX_VALUE);

        try (Txn<ByteBuf> txn = env.txnRead();
             Cursor<ByteBuf> cursor = database.openCursor(txn)) {

            if (!cursor.seek(SeekOp.MDB_LAST)) {
                return;
            }

            do  {
                final ByteBuf ret1 = cursor.key();
                //System.out.println(ret1.readLong());
            } while (cursor.prev());

            txn.commit();
        }
   }
    **/
}
