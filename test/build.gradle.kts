import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("com.google.protobuf") version "0.8.17"
}

description = "test"

val junitVersion = "5.7.1"
dependencies {
    //-----------------------------
    protobuf(files("${project.projectDir}/src/test/resources/proto"))

    testImplementation("ch.qos.logback:logback-classic:1.2.3")
    testImplementation("org.fusesource.jansi:jansi:1.14")
    implementation("net.jqwik:jqwik:1.2.0")
    implementation("xerces:xerces:2.4.0")



    testImplementation(project(":utils"))
    implementation(project(":infrastructure"))
    implementation(project(":runtime"))
    implementation(project(":corfudb-tools"))
    implementation(project(":annotations"))
    implementation(project(":annotation-processor"))
    implementation(project(":corfudb-common"))

    implementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
    implementation("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
    implementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
    implementation("org.junit.vintage:junit-vintage-engine:${junitVersion}")
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1"
    }
}

sourceSets {
    main {
        java {
            srcDirs("$buildDir/generated/source/proto/main/java")
        }
    }
}

