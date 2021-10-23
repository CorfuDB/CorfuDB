
import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf") version "0.8.17"
}

val lombokVersion = "1.18.18"
dependencies {
    protobuf(files("${project.projectDir}/proto"))
    implementation(project(":runtime"))
    implementation("org.immutables:gson:2.6.1")
    implementation("org.immutables:value:2.6.1")
    implementation("com.google.protobuf:protobuf-java:3.6.1")
}

description = "Corfu Utils"

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

java {
    withJavadocJar()
}
