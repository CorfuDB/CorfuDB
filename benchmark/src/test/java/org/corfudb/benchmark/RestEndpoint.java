package org.corfudb.benchmark;

import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.selection.Selection;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.corfudb.benchmark.TestRunner.DB_URL;
import static org.corfudb.benchmark.TestRunner.MASTER_BRANCH;
import static org.corfudb.benchmark.TestRunner.REPO_PATH;
import static org.corfudb.benchmark.TestRunner.REPO_URL;
import static org.corfudb.benchmark.TestRunner.openRepo;

@Slf4j
public class RestEndpoint {

    public static int DEFAULT_COMMIT_COUNT = 5;
    public static int NAME_IDX = 0;
    public static int INDEX_IDX = 1;
    public static int FIRST_ROW = 0;

    private static String getName(Row row) {
        return row.getString(0);
    }

    private static String getHash(Row row) {
        return row.getString(1);
    }

    private static Double getValue(Row row) {
        return row.getDouble(2);
    }

    /**
     * Render data from the benchmarks DB. The data is transformed such
     * that each commit becomes a row.
     *
     * @param filter a filter that determines which rows should be filtered
     * @param transform an optional function that transforms the final table
     * @return string represnation of the rendered data
     * @throws SQLException
     * @throws GitAPIException
     * @throws IOException
     */
    public static String renderData(int numOfCommits, Predicate<String> filter,
                                    Optional<Consumer<Table>> transform)
            throws SQLException, GitAPIException, IOException {

        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            log.info("Opened the DB...");
            ResultSet resultSet = connection.createStatement()
                    .executeQuery("select * from benchmarks");
            Table rawTable = Table.read().db(resultSet);

            Column<?> names = rawTable.column(0).unique()
                    .setName("Name").asStringColumn().filter(filter);
            names.sortDescending();

            Table table = Table.create("Results");
            table.addColumns(names);
            table.addColumns(IntColumn.indexColumn("Index", table.rowCount(), 0));
            List<String> commits = getLastCommits(numOfCommits);
            commits.forEach(hash -> {
                log.info("Adding column {}", hash);
                table.addColumns(DoubleColumn.create(hash));
            });

            rawTable.stream()
                    .filter(row -> filter.test(getName(row)))
                    .filter(row -> commits.contains(getHash(row)))
                    .forEach(row -> {

                BigDecimal score = new BigDecimal(getValue(row))
                        .setScale(1, RoundingMode.HALF_DOWN);
                Selection selection = table.stringColumn("Name").isEqualTo(getName(row));
                int rowIndex = table.where(selection).row(FIRST_ROW).getInt(INDEX_IDX);
                table.row(rowIndex).setDouble(getHash(row), score.doubleValue());
            });

            transform.ifPresent(fun -> fun.accept(table));
            return table.printAll();
        }
    }

    /**
     * Represent the values in the provided table as normalized values.
     * Normalization is represented as % from the minimum value.
     *
     * @param table to normalize
     */
    public static void normalize(Table table) {
        Row row = table.row(-1);
        while (row.hasNext()) {
            row.next();
            double min = Double.NaN;
            System.out.println(row.getString(0));
            for (int idx = 2; idx < row.columnCount(); idx++) {
                double value = row.getDouble(idx);
                if (!Double.isNaN(value)) {
                    if (Double.isNaN(min)) {
                        min = value;
                    }

                    if (value < min) {
                        min = value;
                    }
                }
            }

            for (int idx = 2; idx < row.columnCount(); idx++) {
                double value = row.getDouble(idx);
                if (!Double.isNaN(value) && value != 0) {
                    row.setDouble(idx, Math.round(((value - min) / min) * 100 * 10.0) / 10.0);
                }
            }
        }
    }

    public static void startEndpoint() {
        log.info("Starting Javalin...");
        Javalin app = Javalin.create(config -> config.enableDevLogging()).start("0.0.0.0", 8080);
        app.get("/", ctx -> ctx.result(renderData(
                ctx.queryParamAsClass("size", Integer.class).getOrDefault(DEFAULT_COMMIT_COUNT),
                name -> !name.endsWith("gc_score"), Optional.empty())));
        app.get("/all", ctx -> ctx.result(renderData(
                ctx.queryParamAsClass("size", Integer.class).getOrDefault(DEFAULT_COMMIT_COUNT),
                name -> true, Optional.empty())));
        app.get("/normalize", ctx -> ctx.result(renderData(
                ctx.queryParamAsClass("size", Integer.class).getOrDefault(DEFAULT_COMMIT_COUNT),
                name -> !name.endsWith("gc_score"), Optional.of(RestEndpoint::normalize))));
    }

    public static List<String> getLastCommits(int numOfCommits) throws GitAPIException, IOException {
        try (Git repo = openRepo(REPO_PATH, REPO_URL)) {
            return StreamSupport.stream(repo.log()
                            .add(repo.getRepository().resolve(MASTER_BRANCH))
                            .call().spliterator(), false).limit(numOfCommits)
                    .map(revCommit -> revCommit.abbreviate(7).name())
                    .collect(Collectors.toList());
        }
    }

    public static void main(String[] args) throws SQLException, GitAPIException, IOException {
        startEndpoint();
    }
}
