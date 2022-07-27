package org.corfudb.benchmark;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.PruneType;
import com.github.dockerjava.core.DockerClientBuilder;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.corfudb.common.util.ClassUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Slf4j
public class TestRunner {
    public final static String DB_URL = String.format("jdbc:sqlite:%s/benchmarks.db",
            System.getProperty("user.home"));
    public final static String REPO_URL = "https://github.com/CorfuDB/CorfuDB.git";
    public final static String REPO_PATH = String.format("%s/CorfuDB-Target",
            System.getProperty("user.home"));
    private final static String BENCHMARK_DIR = "benchmark";
    private final static String RESULTS_PATH = "benchmark-results";
    private final static String TARGET_DIR = "target";
    private final static String HEAD = "HEAD";

    public final static String MASTER_BRANCH = "refs/heads/master";
    private final static String REPO_BRANCH = "refs/remotes/origin/regression-framework";
    private final static String TEST_JAR = "benchmark-shaded-tests.jar";

    private final static String OPTION_COMMIT = "commit";

    private final static ImmutableList<String> BUILD_CMD = ImmutableList.of("./mvnw", "clean", "install", "-DskipTests");
    private final static ImmutableList<String> DOCKER_BUILD_CMD = ImmutableList.of(".ci/infrastructure-docker-build.sh", "docker");
    private final static ImmutableList<String> PACKAGE_CMD = ImmutableList.of("../mvnw", "package", "-DskipTests");
    private final static ImmutableList<String> REMOVE_RESULTS = ImmutableList.of("rm", "-rf",
            Paths.get(REPO_PATH, BENCHMARK_DIR, "benchmark-results").toString());
    private final static ImmutableList<String> PREPARE_RESULTS = ImmutableList.of("mkdir",
            Paths.get(REPO_PATH, BENCHMARK_DIR, "benchmark-results").toString());
    private final static ImmutableList<String> RUN_BENCHMARK_CMD = ImmutableList.of("java", "-cp", "target/benchmark-shaded-tests.jar:target/benchmark-shaded.jar");

    /**
     * Open/clone the specified Git URL on the given path.
     *
     * @param repoPath file path where the repo should be cloned/opened
     * @param repoUrl the remote URL of the repo
     * @return {@link Git} object
     * @throws IOException
     * @throws GitAPIException
     */
    protected static Git openRepo(String repoPath, String repoUrl) throws IOException, GitAPIException {
        if (Files.exists(Paths.get(repoPath))) {
            log.info("Opening the existing repo {}...", repoPath);
            return Git.open(Paths.get(repoPath, ".git").toFile());
        } else {
            log.info("Cloning the repo {}...", repoUrl);
            return Git.cloneRepository()
                    .setURI(repoUrl)
                    .setDirectory(Paths.get(repoPath).toFile())
                    .call();
        }
    }

    /**
     * Checkout the specified commit/branch within the provided Git repository.
     *
     * @param repo repository containing the commit/branch
     * @param commit the commit/branch that needs to be checked out
     * @return hash of the commit
     * @throws GitAPIException
     * @throws IOException
     */
    private static String checkoutBranch(Git repo, String commit) throws GitAPIException, IOException {
        log.info("Checkout out {}", MASTER_BRANCH);
        repo.checkout().setName(MASTER_BRANCH).call();
        repo.pull().call();

        ObjectId commitId = repo.getRepository().resolve(commit);
        repo.checkout().setName(commitId.getName()).call();
        return commitId.abbreviate(7).name();
    }

    /**
     * Apply the regression framework path on the specified Git repository.
     *
     * @param repo repository on which the changes should be applied
     * @throws GitAPIException
     */
    private static void applyChanges(Git repo) throws GitAPIException {
        log.info("Cherry-picking {}", REPO_BRANCH);
        ObjectId id = repo.branchList().setListMode(ListMode.REMOTE).call().stream()
                .filter(ref -> Objects.equals(ref.getName(), REPO_BRANCH)).findFirst()
                .orElseThrow(() -> new RuntimeException(REPO_BRANCH)).getObjectId();
        repo.cherryPick().include(id).call();
    }

    /**
     * Execute the provided command within an optional CWD.
     *
     * @param command the command to execute
     * @param directory optional current working directory under which the
     *                  command should be executed
     * @throws IOException
     * @throws InterruptedException
     */
    private static void executeCommand(List<String> command, Optional<Path> directory)
            throws IOException, InterruptedException {
        log.info("Executing {}", command);
        ProcessBuilder processBuilder = new ProcessBuilder().command(command);
        directory.map(Path::toFile).map(processBuilder::directory);

        Process process = processBuilder.start();
        BufferedReader inputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        String line;
        while ((line = inputReader.readLine()) != null) {
            System.out.println(line);
        }

        int exitVal = process.waitFor();
        if (exitVal != 0) {
            while ((line = errorReader.readLine()) != null) {
                System.out.println(line);
            }
            throw new RuntimeException("Unable to execute the command.");
        }
    }

    /**
     * Cleanup all dangling Docker resources. In some cases when a benchmark
     * is not correctly executed, we might be dealing with lingering resources.
     *
     * @throws IOException
     */
    public static void dockerCleanup() throws IOException {
        try (DockerClient dockerClient = DockerClientBuilder.getInstance().build()) {
            dockerClient.pruneCmd(PruneType.NETWORKS).exec();
            dockerClient.pruneCmd(PruneType.CONTAINERS).exec();
        }
    }

    /**
     * Given the file path to a JAR containing all benchmarks, extract the benchmark
     * classes into a List<String>.
     *
     * @param filePath file path to the JAR containing benchmarks.
     * @return a list of benchmarks
     * @throws IOException
     */
    private static List<String> extractBenchmarks(Path filePath) throws IOException {
        ZipFile zipFile  = new ZipFile(filePath.toFile());
        List<String> fileContent = zipFile.stream()
                .map(ZipEntry::getName)
                .filter(name -> name.endsWith("Benchmark.class"))
                .filter(name -> name.startsWith("org/corfudb/benchmark"))
                .map(name -> StringUtils.removeEnd(name, ".class"))
                .collect(Collectors.toList());
        fileContent.forEach(log::info);
        zipFile.close();
        return fileContent;
    }

    /**
     * Parse the results for a single benchmark invocation.
     *
     * @param file a JSON file containing the results.
     * @return a map of String (benchmark result) -> Double (execution time)
     */
    private static Map<String, Double> parseResults(Path file) {
        Gson gson = new Gson();
        log.info("Parsing {}", file.toAbsolutePath());
        final Map<String, Double> results = new HashMap<>();
        try (Reader reader = Files.newBufferedReader(file)) {
            Map<String, ?>[] list = gson.fromJson(reader, Map[].class);
            if (list == null || ArrayUtils.isEmpty(list)) {
                log.warn("Invalid file... skipping.");
                return results;
            }

            for (Map<String, ?> result: list) {
                final Double threadCount = ClassUtils.cast(result.get("threads"));
                final String[] benchmarkName = StringUtils.split((String) result.get("benchmark"), '.');
                final String className = benchmarkName[benchmarkName.length - 2];
                final String functionName = benchmarkName[benchmarkName.length - 1];

                Map<String, String> params = new HashMap<>();
                if (result.get("params") != null) {
                    params = ClassUtils.cast(result.get("params"));
                }
                final String paramString = params.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining("_"));

                final Map<String, Double> primaryMetric = ClassUtils.cast(result.get("primaryMetric"));
                final Double runningTime = primaryMetric.get("score");
                results.put(String.format("%s_%s_threads_%d_%s_score",
                                className, functionName, threadCount.intValue(), paramString),
                        runningTime);

                final Map<String, Map<String, Double>> secondaryMetric = ClassUtils.cast(result.get("secondaryMetrics"));
                if (secondaryMetric.containsKey("·gc.time")) {
                    final Double gcTime = secondaryMetric.get("·gc.time").get("score");
                    results.put(String.format("%s_%s_threads_%d_%s_gc_score",
                                    className, functionName, threadCount.intValue(), paramString),
                            gcTime);
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        results.forEach((key, value) -> log.info("{} = {}", key, value));
        return results;
    }

    /**
     * Initialize the database.
     *
     * @throws SQLException
     */
    public static void createDb() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            Statement create = connection.createStatement();
            create.executeUpdate("create table if not exists benchmarks " +
                    "(name STRING not null, hash TEXT not null, runtime DOUBLE, " +
                    "PRIMARY KEY(name, runtime)" +
                    ")");
        }
    }

    /**
     * Given the result map and the commit hash, populate the database.
     *
     * @param results a map of String -> Double tuples
     * @param hash commit hash associated with the result map
     * @throws SQLException
     */
    public static void populateDb(Map<String, Double> results, String hash) throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            System.out.println(hash);
            for (Map.Entry<String, Double> entry : results.entrySet()) {
                PreparedStatement insert = connection.prepareStatement("INSERT OR REPLACE INTO benchmarks VALUES(?, ?, ?)");
                insert.setString(1, entry.getKey());
                insert.setString(2, hash);
                insert.setDouble(3, entry.getValue());
                insert.executeUpdate();
            }
        }
    }

    /**
     * Check to see if we executed benchmarks against the specified hash.
     *
     * @param hash commit hash
     * @return does the database contain the specified hash
     * @throws SQLException
     */
    public static boolean dbContainsHash(String hash) throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            PreparedStatement exists = connection.prepareStatement(
                    "select * from benchmarks where hash=?");
            exists.setString(1, hash);
            ResultSet result = exists.executeQuery();
            return result.next();
        }
    }

    public static void main(String[] args) throws GitAPIException, IOException, InterruptedException, SQLException, ParseException {
        Options options = new Options();
        Option commit = Option.builder()
                .longOpt(OPTION_COMMIT)
                .argName("Commit ID")
                .hasArg()
                .required(false)
                .desc("Commit to run the benchmark on").build();
        options.addOption(commit);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String commitHash = Optional
                .ofNullable(cmd.getOptionValue(commit))
                .orElse(HEAD);
        Git repo = openRepo(REPO_PATH, REPO_URL);
        String hash = checkoutBranch(repo, commitHash);
        applyChanges(repo);
        createDb();
        if (dbContainsHash(hash)) {
            log.error("Hash {} already present in the DB.", hash);
            return;
        }
        repo.close();

        dockerCleanup();
        executeCommand(BUILD_CMD, Optional.of(Paths.get(REPO_PATH)));
        executeCommand(DOCKER_BUILD_CMD, Optional.of(Paths.get(REPO_PATH)));
        executeCommand(PACKAGE_CMD, Optional.of(Paths.get(REPO_PATH, BENCHMARK_DIR)));
        executeCommand(REMOVE_RESULTS, Optional.of(Paths.get(REPO_PATH, BENCHMARK_DIR)));
        executeCommand(PREPARE_RESULTS, Optional.of(Paths.get(REPO_PATH, BENCHMARK_DIR)));
        List<String> benchmarks = extractBenchmarks(Paths.get(REPO_PATH, BENCHMARK_DIR, TARGET_DIR, TEST_JAR));
        for (String benchmark: benchmarks) {
            executeCommand(
                    ImmutableList.<String>builder().addAll(RUN_BENCHMARK_CMD).add(benchmark).build(),
                    Optional.of(Paths.get(REPO_PATH, BENCHMARK_DIR)));
        }
        try (Stream<Path> stream = Files.list(Paths.get(REPO_PATH, BENCHMARK_DIR, RESULTS_PATH))) {
            Map<String, Double> results = stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(TestRunner::parseResults)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            populateDb(results, hash);
        }
    }
}
