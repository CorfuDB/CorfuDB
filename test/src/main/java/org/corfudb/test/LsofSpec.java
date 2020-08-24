package org.corfudb.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@Slf4j
public class LsofSpec {

    public void check(Path fileName) throws IOException {
        Process process = new ProcessBuilder()
                .command("lsof")
                .start();

        String output;
        try (InputStream inputStream = process.getInputStream()) {
            output = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
        }

        List<String> leaks = Arrays
                .stream(output.split("\\r?\\n"))
                .filter(record -> record.contains(fileName.toString()))
                .collect(Collectors.toList());

        if (!leaks.isEmpty()) {
            log.error("File descriptor leaks detected.");
            fail("File descriptor leaks error: " + leaks);
        }
    }
}
