import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf") version "0.8.17"
}

val grpcVersion = "1.41.0"
dependencies {

    protobuf(files("${project.projectDir}/proto"))

    implementation("org.roaringbitmap:RoaringBitmap:0.8.13")
    implementation("org.ehcache:sizeof:0.3.0")
    implementation("com.google.code.gson:gson:2.8.0")


    implementation("io.micrometer:micrometer-core:1.5.5")
    implementation("io.grpc:grpc-netty-shaded:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")

    //------------------------------------------------------------------
    implementation(project(":corfudb-common"))
    implementation(project(":runtime"))
    implementation(project(":utils"))
    implementation("com.offbytwo:docopt:0.6.0.20150202")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("com.github.ben-manes.caffeine:caffeine:2.8.5")
    testImplementation("org.mockito:mockito-inline:3.6.0")
    testImplementation("org.assertj:assertj-core:3.19.0")
}

description = "infrastructure"

protobuf {

    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.41.0"
        }
    }

    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs("$buildDir/generated/source/proto/main/java")
        }
    }
}
