import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf") version "0.8.17"
    id("com.gorylenko.gradle-git-properties") version "2.3.1"
}

val lombokVersion = "1.18.18"

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs = listOf("-XDignore.symbol.file=true")
}

dependencies {
    implementation("org.roaringbitmap:RoaringBitmap:0.8.13")
    implementation("io.micrometer:micrometer-core:1.5.5")
    implementation("io.micrometer:micrometer-registry-prometheus:1.5.5")
    //--------------------------------------------------------------------------
    implementation(project(":corfudb-common"))
    implementation(project(":annotations"))
    implementation(project(":annotation-processor"))

    protobuf(files("${project.projectDir}/proto"))

    implementation("com.esotericsoftware:kryo:4.0.0")
    implementation("org.codehaus.plexus:plexus-utils:3.0.24")
    implementation("de.javakaffee:kryo-serializers:0.41")
    implementation("com.google.code.gson:gson:2.8.0")
    implementation("org.javassist:javassist:3.21.0-GA")
    implementation("org.apache.commons:commons-lang3:3.9")
    implementation("org.apache.commons:commons-compress:1.21")
    implementation("org.rocksdb:rocksdbjni:6.8.1")
    implementation("com.google.protobuf:protobuf-java-util:3.6.1")
    implementation("io.grpc:grpc-netty-shaded:1.41.0")
    implementation("io.grpc:grpc-protobuf:1.41.0")
    implementation("io.grpc:grpc-stub:1.41.0")
    implementation("org.ehcache:sizeof:0.4.0")
    implementation("commons-collections:commons-collections:3.2.2")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
}

dependencies {
    implementation("com.google.protobuf:protobuf-java:3.6.1")
}



sourceSets {
    main {
        java {
            srcDirs("$buildDir/generated/source/proto/main/java")
        }
    }
}

description = "runtime"

protobuf {

    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1"
    }
}

configure<com.gorylenko.GitPropertiesPluginExtension> {
    customProperty("git.commit.id.describe-short", "CorfuDb")
    customProperty("git.build.time", "123")
    //(dotGitDirectory as DirectoryProperty).set("${project.rootDir}/../somefolder/.git")
}

