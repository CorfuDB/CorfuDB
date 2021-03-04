FROM openjdk:8-jdk-alpine3.8

WORKDIR /app

COPY bin/ /app/
COPY target/LongevityRun.jar /app/LongevityRun.jar

CMD java -cp ./*.jar org.corfudb.generator.LongevityRun
