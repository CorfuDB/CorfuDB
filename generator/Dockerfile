FROM eclipse-temurin:25-jdk-noble

WORKDIR /app

COPY bin/ /app/
COPY target/LongevityRun.jar /app/LongevityRun.jar

CMD java -cp ./*.jar org.corfudb.generator.LongevityRun
