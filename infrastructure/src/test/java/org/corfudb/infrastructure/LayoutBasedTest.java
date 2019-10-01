package org.corfudb.infrastructure;

import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class LayoutBasedTest {
    private static String servers = "A|B|C";

    public Layout createTestLayout(List<Layout.LayoutSegment> segments){

        List<String> s = Arrays.stream(servers.split("|")).collect(Collectors.toList());
        long epoch = 0L;
        UUID uuid = UUID.randomUUID();
        return new Layout(s, s, segments, new ArrayList<>(), epoch, uuid);
    }

}
