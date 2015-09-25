package org.corfudb.runtime.stream;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.view.*;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;

import java.util.UUID;

/**
 * Created by mwei on 4/30/15.
 */
public class SimpleStreamTest {

    SimpleStream s;
    CorfuDBRuntime cdr;
    ICorfuDBInstance instance;

    @Before
    public void generateStream()
    {
        MemoryConfigMasterProtocol.inMemoryClear();
        cdr = CorfuDBRuntime.createRuntime("memory");
        instance = cdr.getLocalInstance();
        s = (SimpleStream) instance.openStream(UUID.randomUUID());
    }

  //  @Test
    public void streamIsReadableWritable() throws Exception
    {
        s.append("hello world");
        assertEquals(s.readNextObject(), "hello world");
    }

  //  @Test
    public void emptyStreamReturnsNull() throws Exception
    {
        assertNull(s.readNextEntry());
        assertNull(s.readNextObject());
    }

  //  @Test
    public void streamHasCorrectUUID() throws Exception
    {
        UUID uuid = UUID.randomUUID();
        SimpleStream s2 = (SimpleStream) instance.openStream(uuid);
        assertEquals(uuid, s2.getStreamID());
    }

 //   @Test
    public void multipleWritesAndReads() throws Exception
    {
        s.append("hello world 0");
        s.append("hello world 1");
        s.append("hello world 2");
        assertEquals(s.readNextObject(), "hello world 0");
        assertEquals(s.readNextObject(), "hello world 1");
        assertEquals(s.readNextObject(), "hello world 2");
        s.append("hello world 3");
        assertEquals(s.readNextObject(), "hello world 3");
    }

  //  @Test
    public void multipleWritesAndReadsFromDifferentStreams() throws Exception
    {
        SimpleStream s2 = (SimpleStream) instance.openStream(UUID.randomUUID());
        SimpleStream s3 = (SimpleStream) instance.openStream(UUID.randomUUID());

        s.append("hello world 0");
        assertNull(s2.readNextObject());
        assertNull(s3.readNextObject());
        s2.append("hello world 1");
        assertEquals("hello world 0", s.readNextObject());
        assertNull(s.readNextObject());
        assertNull(s3.readNextObject());
        assertEquals("hello world 1", s2.readNextObject());
        s3.append("hello world 2");
        assertEquals("hello world 2", s3.readNextObject());
        assertNull(s2.readNextObject());
        assertNull(s3.readNextObject());
    }

 //   @Test
    public void holesResultInException() throws Exception
    {
        s.append("hello world 0");
        instance.getStreamingSequencer().getNext(s.getStreamID(), 1);
        s.append("hello world 1");
        assertEquals(s.readNextObject(), "hello world 0");
        assertRaises(s::readNextObject, HoleEncounteredException.class);
    }

  //  @Test
    public void entriesAreOrdered() throws Exception
    {
        SimpleStream s2 = (SimpleStream)  instance.openStream(UUID.randomUUID());
        ITimestamp t1 = s.append("hello world 0");
        ITimestamp t2 = s2.append("hello world 1");
        ITimestamp t3 = s.append("hello world 2");

        IStreamEntry e1 = s.readNextEntry();
        IStreamEntry e2 = s2.readNextEntry();
        IStreamEntry e3 = s.readNextEntry();

        assertThat(t1)
                .isEqualTo(t1)
                .isLessThan(t2)
                .isLessThan(t3)
                .isEqualTo(e1.getTimestamp())
                .isLessThan(e2.getTimestamp())
                .isLessThan(e3.getTimestamp());

        assertThat(t2)
                .isEqualTo(t2)
                .isGreaterThan(t1)
                .isLessThan(t3)
                .isGreaterThan(e1.getTimestamp())
                .isEqualTo(e2.getTimestamp())
                .isLessThan(e3.getTimestamp());

        assertThat(t3)
                .isEqualTo(t3)
                .isGreaterThan(t1)
                .isGreaterThan(t2)
                .isGreaterThan(e1.getTimestamp())
                .isGreaterThan(e2.getTimestamp())
                .isEqualTo(e3.getTimestamp());

        assertThat(e1)
                .isEqualTo(e1)
                .isLessThan(e2)
                .isLessThan(e3);

        assertThat(e2)
                .isEqualTo(e2)
                .isGreaterThan(e1)
                .isLessThan(e3);

        assertThat(e3)
                .isEqualTo(e3)
                .isGreaterThan(e1)
                .isGreaterThan(e2);
    }

 //   @Test
    public void entriesAreLinearizable() throws Exception
    {
        SimpleStream s2 = (SimpleStream)  instance.openStream(UUID.randomUUID());
        SimpleStream s3 = (SimpleStream)  instance.openStream(UUID.randomUUID());
        ITimestamp t1 = s.append("hello world 0");
        ITimestamp t2 = s2.append("hello world 1");
        ITimestamp t3 = s.append("hello world 2");
        ITimestamp linearizationPoint = s.check();
        ITimestamp t4 = s.append("hello world 3");
        ITimestamp t5 = s2.append("hello world 4");

        ITimestamp chk1 = s.getCurrentPosition();
        ITimestamp chk1_2 = s2.getCurrentPosition();
        s.readNextEntry(); // "hello world 0"
        ITimestamp chk2 = s.getCurrentPosition();
        ITimestamp chk2_2 = s2.getCurrentPosition();
        s2.readNextEntry(); // "hello world 1"
        ITimestamp chk3 = s.getCurrentPosition();
        ITimestamp chk3_2 = s2.getCurrentPosition();
        s.readNextEntry(); // "hello world 2"
        ITimestamp chk4 = s.getCurrentPosition();
        ITimestamp chk4_2 = s2.getCurrentPosition();
        s.readNextEntry(); // "hello world 3"
        ITimestamp chk5 = s.getCurrentPosition();
        ITimestamp chk5_2 = s2.getCurrentPosition();
        s2.readNextEntry(); // "hello world 4"
        ITimestamp chk6 = s.getCurrentPosition();
        ITimestamp chk6_2 = s2.getCurrentPosition();

        //relative to append timestamps
        assertThat(linearizationPoint)
                .isGreaterThan(t1)
                .isGreaterThan(t2)
                .isLessThanOrEqualTo(t3)
                .isLessThanOrEqualTo(t4)
                .isLessThanOrEqualTo(t5);

        //relative to position during read
        assertThat(linearizationPoint)
                .isGreaterThan(chk1)
                .isGreaterThan(chk2)
                .isGreaterThan(chk3)
                .isLessThanOrEqualTo(chk4)
                .isLessThanOrEqualTo(chk5)
                .isLessThanOrEqualTo(chk6);

        //relative to position during read of s2
        assertThat(linearizationPoint)
                .isGreaterThan(chk1_2)
                .isGreaterThan(chk2_2)
                .isGreaterThan(chk3_2)
                .isGreaterThan(chk4_2)
                .isGreaterThan(chk5_2)
                .isLessThanOrEqualTo(chk6);
    }
}
