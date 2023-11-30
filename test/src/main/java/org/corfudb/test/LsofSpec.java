package org.corfudb.test;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@Slf4j
public class LsofSpec {

    public void check(Path fileName) throws IOException {
        Process process = new ProcessBuilder()
                .command("lsof", fileName.toAbsolutePath().toString())
                .start();

        String output;

        ArrayList<String> objects = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            while ((output = bufferedReader.readLine()) != null) {
                objects.add(output);
            }

            List<String> leaks = objects.stream()
                    .filter(record -> record.contains(fileName.toString()))
                    .collect(Collectors.toList());

            if (!leaks.isEmpty()) {
                log.error("File descriptor leaks detected.");
                fail("File descriptor leaks error: " + leaks);
            }
        }
    }
}
