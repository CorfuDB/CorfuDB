FROM openjdk:8-jdk-alpine3.8

ARG CORFU_JAR
ARG CMDLETS_JAR
ARG CORFU_TOOLS_JAR

WORKDIR /app

RUN apk add --update iptables bash jq python3 sudo

COPY target/${CORFU_JAR} /usr/share/corfu/lib/${CORFU_JAR}
COPY target/${CMDLETS_JAR} /usr/share/corfu/lib/${CMDLETS_JAR}
COPY target/${CORFU_TOOLS_JAR} /usr/share/corfu/lib/${CORFU_TOOLS_JAR}

COPY target/bin /usr/share/corfu/bin
COPY target/corfu_scripts /usr/share/corfu/corfu_scripts
COPY target/scripts /usr/share/corfu/scripts

# For integration testing purposes
COPY target/${CORFU_JAR} /app/corfu.jar

CMD java -cp *.jar org.corfudb.infrastructure.CorfuServer
