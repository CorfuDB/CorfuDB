FROM openjdk:8-jdk-alpine3.8

ARG CORFU_JAR
WORKDIR /app

RUN apk add --update iptables sudo

ADD target/${CORFU_JAR} /app/corfu.jar

CMD java -cp *.jar org.corfudb.infrastructure.CorfuServer
