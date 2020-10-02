package org.corfudb.common.metrics.micrometer.loggingsink;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.corfudb.common.metrics.micrometer.protocoltransformer.LineTransformer;
import org.corfudb.common.metrics.micrometer.protocoltransformer.influx.ByteDistSummaryInfluxLineTransformer;
import org.corfudb.common.metrics.micrometer.protocoltransformer.influx.CounterInfluxLineTransformer;
import org.corfudb.common.metrics.micrometer.protocoltransformer.influx.GaugeInfluxLineTransformer;
import org.corfudb.common.metrics.micrometer.protocoltransformer.influx.LongRunningTaskInfluxLineTransformer;
import org.corfudb.common.metrics.micrometer.protocoltransformer.influx.TimerInfluxLineTransformer;
import org.slf4j.Logger;

/**
 * This sink prints the lines to the log in the InfluxDb line protocol.
 * https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
 */
@AllArgsConstructor
public class InfluxLineProtocolLoggingSink implements LoggingSink {

    @NonNull
    private final Logger logger;
    private final LineTransformer lineTransformer = new LineTransformer(
            ImmutableList.of(
                    new GaugeInfluxLineTransformer(),
                    new CounterInfluxLineTransformer(),
                    new TimerInfluxLineTransformer(),
                    new ByteDistSummaryInfluxLineTransformer(),
                    new LongRunningTaskInfluxLineTransformer())
    );

    @Override
    public void accept(String s) {
        lineTransformer.transformLine(s).ifPresent(logger::debug);
    }
}
