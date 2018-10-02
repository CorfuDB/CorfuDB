package org.corfudb.generator.replayer;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A convenience class contains helper methods for retrieving and exporting events.
 * Created by Sam Behnam on 1/18/18.
 */
@Slf4j
public class ReplayerUtil {

    public static void export(String pathToFile) throws IOException {
        final List<Event> eventList = load(pathToFile);
        export(eventList, pathToFile + ".txt");
    }

    public static void export(List<Event> eventList, String pathToFile) throws IOException {
        try (PrintStream printStream = new PrintStream(new File(pathToFile))) {
            for (Event event : eventList) {
                printStream.println(event.toString());
            }
        }
    }

    public static List<Event> load(String pathToFile) {
        List<Event> eventList = Collections.emptyList();
        try (FileInputStream fileInputStream = new FileInputStream(pathToFile);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)){
            eventList = (List<Event>) objectInputStream.readObject();
        } catch (IOException |
                 ClassNotFoundException e) {
            log.error("Error loading files", e);
            System.exit(1);
        }
        return eventList;
    }

    public static List<Event> loadAll(List<String> pathToFiles) {
        List<Event> eventList = Collections.emptyList();
        for (String pathToFile : pathToFiles) {
            eventList.addAll(load(pathToFile));
        }
        return eventList;
    }

    public static List<String> retrieveListOfPathToEventQueues(final String persistedListOfEventListFiles) {
        final List<String> pathsToEventQueues = new ArrayList<>();

        try (InputStream inputStream = new FileInputStream(persistedListOfEventListFiles);
             InputStreamReader reader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(reader)){
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                pathsToEventQueues.add(line);
            }

        } catch (IOException e) {
            log.error("Error retrieving events", e);
            System.exit(1);
        }
        return pathsToEventQueues;
    }
}
