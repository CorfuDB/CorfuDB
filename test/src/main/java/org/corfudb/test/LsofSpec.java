package org.corfudb.test;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class LsofSpec {

    public void check() throws Exception {
        Process process = new ProcessBuilder()
                .command("lsof")
                .start();

        InputStream inputStream = process.getInputStream();
        String output = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
        inputStream.close();

        List<String> leaks = Arrays
                .stream(output.split("\\r?\\n"))
                .filter(record -> record.contains("log/0.log"))
                .collect(Collectors.toList());

        if (!leaks.isEmpty()){
            fail("File descriptor leaks detected: " + leaks);
        }
    }
}
