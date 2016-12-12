package org.corfudb.util.quickcheck;

import org.corfudb.runtime.CorfuRuntime;

import java.util.Map;

class QCUtil {
    public static CorfuRuntime configureRuntime(Map<String, Object> opts) {
        return new CorfuRuntime()
                .parseConfigurationString((String) opts.get("--config"))
                .connect();
    }

    public static String[] replyOk(String... args) {
        return makeStringArray("OK", args);
    }

    public static String[] replyErr(String... args) {
        return makeStringArray("ERROR", args);
    }

    private static String[] makeStringArray(String zeroth, String... args) {
        String[] a = new String[args.length + 1];
        for (int i = 0; i < args.length; i++) {
            a[i + 1] = args[i];
        }
        a[0] = zeroth;
        return a;
    }
}
