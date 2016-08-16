package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.TokenRequestMsg;
import org.corfudb.protocols.wireprotocol.TokenResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.SequencerServerAssertions.assertThat;

/**
 * Created by mwei on 12/13/15.
 */
public class SequencerServerTest extends AbstractServerTest {

    public SequencerServerTest() {
        super();
    }

    @Override
    public AbstractServer getDefaultServer() {
        return new
                SequencerServer(ServerContextBuilder.emptyContext());
    }

    @Test
    public void responseForEachRequest() {
        for (int i = 0; i < 100; i++) {
            sendMessage(new TokenRequestMsg(Collections.<UUID>emptySet(), 1));
            assertThat(getResponseMessages().size())
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void tokensAreIncreasing() {
        long lastToken = -1;
        for (int i = 0; i < 100; i++) {
            sendMessage(new TokenRequestMsg(Collections.<UUID>emptySet(), 1));
            long thisToken = getLastMessageAs(TokenResponseMsg.class).getToken();
            assertThat(thisToken)
                    .isGreaterThan(lastToken);
            lastToken = thisToken;
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < 100; i++) {
            sendMessage(new TokenRequestMsg(Collections.<UUID>emptySet(), 1));
            long thisToken = getLastMessageAs(TokenResponseMsg.class).getToken();

            sendMessage(new TokenRequestMsg(Collections.<UUID>emptySet(), 0));
            long checkToken = getLastMessageAs(TokenResponseMsg.class).getToken();

            assertThat(thisToken)
                    .isEqualTo(checkToken);
        }
    }

    @Test
    public void perStreamCheckTokenPositionWorks() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < 100; i++) {
            sendMessage(new TokenRequestMsg(Collections.singleton(streamA), 1));
            long thisTokenA = getLastMessageAs(TokenResponseMsg.class).getToken();

            sendMessage(new TokenRequestMsg(Collections.singleton(streamA), 0));
            long checkTokenA = getLastMessageAs(TokenResponseMsg.class).getToken();

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new TokenRequestMsg(Collections.singleton(streamB), 1));
            long thisTokenB = getLastMessageAs(TokenResponseMsg.class).getToken();

            sendMessage(new TokenRequestMsg(Collections.singleton(streamB), 0));
            long checkTokenB = getLastMessageAs(TokenResponseMsg.class).getToken();

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            sendMessage(new TokenRequestMsg(Collections.singleton(streamA), 0));
            long checkTokenA2 = getLastMessageAs(TokenResponseMsg.class).getToken();

            assertThat(checkTokenA2)
                    .isEqualTo(checkTokenA);

            assertThat(thisTokenB)
                    .isGreaterThan(checkTokenA2);
        }
    }

    @Test
    public void checkSequencerCheckpointingWorks()
            throws Exception {
        String serviceDir = getTempDir();

        SequencerServer s1 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(0)
                .setCheckpoint(1)
                .build());

        this.router.reset();
        this.router.addServer(s1);
        sendMessage(new TokenRequestMsg(Collections.singleton(CorfuRuntime.getStreamID("a")), 1));
        sendMessage(new TokenRequestMsg(Collections.singleton(CorfuRuntime.getStreamID("a")), 1));
        assertThat(s1)
                .tokenIsAt(2);
        Thread.sleep(1400);
        s1.shutdown();

        SequencerServer s2 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(-1)
                .setCheckpoint(1)
                .build());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2)
                .tokenIsAt(2);
    }

}
