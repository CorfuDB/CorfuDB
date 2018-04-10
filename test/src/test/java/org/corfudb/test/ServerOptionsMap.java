package org.corfudb.test;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.EventLoopGroup;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Builder.Default;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

@Builder
public class ServerOptionsMap {

    @Retention(RetentionPolicy.RUNTIME)
    @interface OptionName {
        String name();
    }

    @OptionName(name="initial-token")
    @Default long initialToken = 0L;

    @OptionName(name="single")
    @Default boolean single = true;

    @OptionName(name="memory")
    @Default boolean memory = true;

    @OptionName(name="log-path")
    @Default String logPath = null;

    @OptionName(name="no-verify")
    @Default boolean noVerify = false;

    @OptionName(name="enable-tls")
    @Default boolean tlsEnabled = false;

    @OptionName(name="enable-tls-mutual-auth")
    @Default boolean tlsMutualAuthEnabled = false;

    @OptionName(name="tls-protocols")
    @Default String tlsProtocols = "";

    @OptionName(name="tls-ciphers")
    @Default String tlsCiphers = "";

    @OptionName(name="keystore")
    @Default String keystore = "";

    @OptionName(name="keystore-password-file")
    @Default String keystorePasswordFile = "";

    @OptionName(name="enable-sasl-plain-text-auth")
    @Default boolean saslPlainTextAuth = false;

    @OptionName(name="truststore")
    @Default String truststore = "";

    @OptionName(name="truststore-password-file")
    @Default String truststorePasswordFile = "";

    @OptionName(name="implementation")
    @Default String implementation = "local";

    @OptionName(name="cache-heap-ratio")
    @Default String cacheSizeHeapRatio = "0.5";

    @OptionName(name="address")
    @Default String address = "server";

    @OptionName(name="<port>")
    @Default String port = "9000";

    @OptionName(name="sequencer-cache-size")
    @Default String seqCache = "1000";

    @OptionName(name="management-server")
    @Default String managementBootstrapEndpoint = null;

    @OptionName(name="Threads")
    @Default String numThreads = "0";

    @OptionName(name="HandshakeTimeout")
    @Default String handshakeTimeout = "10";

    @OptionName(name="Prefix")
    @Default String prefix = "";

    @OptionName(name="cluster-id")
    @Default String clusterId = "auto";

    @OptionName(name="boss")
    EventLoopGroup bossGroup;

    @OptionName(name="client")
    EventLoopGroup clientGroup;

    @OptionName(name="worker")
    EventLoopGroup workerGroup;

    public Map<String, Object> toMap() {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        Arrays.stream(this.getClass().getDeclaredFields())
            .filter(f -> f.getAnnotation(OptionName.class) != null)
            .forEach(f ->insertFieldToBuilder(f, builder));

        return builder.build();
    }

    private void insertFieldToBuilder(@Nonnull Field field,
        @Nonnull ImmutableMap.Builder<String, Object> mapBuilder) {
        try {
            if (field.get(this) != null) {
                String optionName = field.getAnnotation(OptionName.class).name();

                // If the option is not < for port and not a EventLoopGroup (test supplied arg)
                // we prepend it with -- for a switch.
                if (!optionName.startsWith("<")
                    && ! field.getType().equals(EventLoopGroup.class)) {
                    optionName = "--" + optionName;
                }

                mapBuilder.put(optionName, field.get(this));
            }
        } catch (IllegalAccessException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }
}