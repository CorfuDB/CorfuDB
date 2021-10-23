
//plugins {
   // https://docs.freefair.io/gradle-plugins/current/reference/
    // id("io.freefair.lombok") version "6.2.0"
//}

val lombokVersion = "1.18.18"
dependencies {
    implementation("org.roaringbitmap:RoaringBitmap:0.8.13")
    implementation("org.ehcache:sizeof:0.3.0")
    implementation("io.micrometer:micrometer-core:1.5.5")
    implementation("io.micrometer:micrometer-registry-prometheus:1.5.5")
    implementation("net.jpountz.lz4:lz4:1.3.0")
    implementation("com.github.luben:zstd-jni:1.3.7-3")
    implementation("io.dropwizard.metrics:metrics-core:4.2.1")
    testImplementation("ch.qos.logback:logback-classic:1.2.3")
}

description = "corfudb-common"

