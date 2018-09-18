package org.corfudb.universe.scenario.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Parses scenario config files
 */
@Slf4j
public class ScenarioConfigParser {
    private static final ScenarioConfigParser INSTANCE = new ScenarioConfigParser();

    private final ObjectMapper objectMapper = new ObjectMapper();

    private ScenarioConfigParser() {
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new GuavaModule());
    }

    public static ScenarioConfigParser getInstance() {
        return INSTANCE;
    }

    /**
     * Load scenarios from a directory
     * @param directory scenarios directory
     * @return list of scenarios
     * @throws URISyntaxException throws if scenario directory doesn't exists
     */
    public List<ScenarioConfig> load(String directory) throws URISyntaxException {
        File[] scenarioFiles = Objects.requireNonNull(
                new File(ClassLoader.getSystemResource(directory).toURI()).listFiles()
        );

        return Arrays.stream(scenarioFiles)
                .map(scenario -> {
                    try {
                        return objectMapper.readValue(scenario, ScenarioConfig.class);
                    } catch (IOException e) {
                        throw new IllegalStateException("Can't load scenario: " + scenario.getPath(), e);
                    }
                })
                .collect(Collectors.toList());
    }
}
