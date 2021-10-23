
dependencies {
    implementation(project(":annotations"))
    implementation("org.rocksdb:rocksdbjni:6.8.1")

    //-----------------------------
    implementation(project(":runtime"))
    implementation("com.offbytwo:docopt:0.6.0.20150202")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("com.google.protobuf:protobuf-java-util:3.6.1")
}

description = "corfudb-tools"

java {
    withJavadocJar()
}
