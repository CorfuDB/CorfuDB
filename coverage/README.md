## Corfu Test Coverage

- Unit Test Coverage only
To see the unit test coverage result, run 
```shell
./mvnw clean test
```
and then open ./coverage/target/site/index.html.
the coverage data is 52% in infrastructure and 75% in runtime

- Test Coverage of Unit Test and Integration Test
To see the test coverage result including unit test and integration test, run
```shell
./mvnw clean verify -P it   
```
and then open ./coverage/target/site/index.html.
the coverage data is 79% in infrastructure and 82% in runtime