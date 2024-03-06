ARG BASE_IMAGE=openjdk:17-jdk-bullseye
FROM $BASE_IMAGE

WORKDIR /app

COPY bin/ /app/
COPY target/LongevityRun.jar /app/LongevityRun.jar

CMD java -cp ./*.jar org.corfudb.generator.LongevityRun
