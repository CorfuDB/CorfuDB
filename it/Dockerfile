FROM openjdk:8-jdk-alpine3.8

ARG CORFU_JAR
ADD target/corfu/${CORFU_JAR} /app/corfu.jar

WORKDIR /app

CMD java -cp *.jar org.corfudb.infrastructure.CorfuServer