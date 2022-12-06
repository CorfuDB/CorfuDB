## Corfu Test Coverage

Corfu uses jacoco plugin to generate static code analysis reports. Since the tests are split across Unit Tests, Integration Tests, and Universe Tests, we aggregate the coverage results from all three and report.

The current code coverage results could be found at [Codacy Project Dashboard](https://app.codacy.com/gh/CorfuDB/CorfuDB/dashboard).

### Unit Test Coverage Report

To see the only unit test coverage result, run -

```shell
./mvnw clean test
```

and then, open: `coverage/target/site/jacoco-unit-tests/index.html`.
The aggregate unit tests xml file is located at: `coverage/target/site/jacoco-unit-tests/jacoco.xml`.

### Aggregate Coverage Report

To see the test coverage results of Unit Tests, Integration Tests, and Universe Tests, run -

```shell
./mvnw -pl :coverage -am clean verify -Dmaven.test.failure.ignore=true -Dcode-coverage=true 
```

and then, open: `coverage/target/site/jacoco-aggregate/index.html`.
The aggregate xml file is located at: `coverage/target/site/jacoco-aggregate/jacoco.xml`.

#### Command info

*   `-pl coverage -am` builds coverage module and also makes its dependencies thereby running their unit tests.

*   Maven test failures can be ignored with `-Dmaven.test.failure.ignore=true` because,
    since we are running 3 suits at once by this command, it can be time-consuming to re-run them
    for every single intermittent/flaky test failures. Moreover, all the 3 suits are ensured to be run successfully
    (with individual retry option) in the GitHub Action files before merging the PR.

*   `-Dcode-coverage=true` sets the activation property to activate `it` and `universe` profiles simultaneously.

*   Tip: `help:active-profiles` instead of "clean verify" lists the activated profiles along with the mvn build order.
