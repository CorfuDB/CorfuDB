import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    idea
    java
    id("com.google.osdetector") version "1.7.0" apply false
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.google.osdetector")

    repositories {
        mavenCentral()
        maven {
            url = uri("https://oss.jfrog.org/artifactory/libs-snapshot")
        }
    }

    group = "org.corfudb"
    version = "0.3.1-SNAPSHOT"
    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        sourceCompatibility = JavaVersion.VERSION_1_8.toString()
        targetCompatibility = JavaVersion.VERSION_1_8.toString()
        options.compilerArgs = listOf("-XDignore.symbol.file")//, "-proc:none")
    }

    val lombokVersion = "1.18.18"
    val junitVersion = "5.7.1"

    val implementation by configurations
    val compileOnly by configurations

    val annotationProcessor by configurations
    val testAnnotationProcessor by configurations

    val testCompileOnly by configurations
    val testImplementation by configurations


    dependencies {
        implementation("org.rocksdb:rocksdbjni:6.8.1")
        implementation("org.roaringbitmap:RoaringBitmap:0.8.13")
        implementation("org.ehcache:sizeof:0.3.0")
        implementation("io.micrometer:micrometer-core:1.5.5")
        implementation("com.google.code.gson:gson:2.8.0")


        //-----------------------------------------
        implementation("org.projectlombok:lombok:${lombokVersion}")
        annotationProcessor("org.projectlombok:lombok:${lombokVersion}")

        testImplementation("org.projectlombok:lombok:${lombokVersion}")
        testAnnotationProcessor("org.projectlombok:lombok:${lombokVersion}")

        implementation("org.apache.commons:commons-lang3:3.9")
        implementation("commons-lang:commons-lang:2.6")
        implementation("commons-lang:commons-lang:2.6")
        implementation("commons-collections:commons-collections:3.2.2")
        implementation("org.apache.commons:commons-compress:1.21")

        implementation("net.openhft:zero-allocation-hashing:0.8")

        implementation("org.slf4j:slf4j-api:1.7.32")
        implementation("com.google.guava:guava:31.0-jre")

        implementation("io.netty:netty-all:4.1.50.Final")
        implementation("io.netty:netty-tcnative:2.0.44.Final")

        implementation("commons-io:commons-io:2.8.0")
        implementation("com.google.protobuf:protobuf-java:3.6.1")

        compileOnly("org.apache.httpcomponents:httpclient:4.5.5")

        testImplementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
        testImplementation("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
        testImplementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
        testImplementation("org.junit.vintage:junit-vintage-engine:${junitVersion}")

        testImplementation("org.mockito:mockito-inline:3.6.0")
        testImplementation("org.assertj:assertj-core:3.19.0")

        testImplementation("org.assertj:assertj-core:3.19.0")
    }

    /**
     * https://docs.gradle.org/current/userguide/java_testing.html
     *
     * https://www.baeldung.com/junit-5-gradle
     */
    tasks.withType<Test> {
        maxHeapSize = "4096m"

        useJUnitPlatform()

        reports {
            junitXml.required.set(true)
            html.required.set(true)
        }

        // Uncomment this if you need to skip tests from the set after first failure. Since Gradle 4.6
        failFast = true

        testLogging {
            events = setOf(STARTED, SKIPPED, PASSED, FAILED)

            showStandardStreams = false

            exceptionFormat = TestExceptionFormat.FULL
            showExceptions = true
            showCauses = true
            showStackTraces = false
        }

        
    }

    tasks.withType<Delete> {
        delete("${projectDir}/target")
        delete("${projectDir}/build")
    }
}