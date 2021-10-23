repositories {
    maven {
        //isAllowInsecureProtocol = true
        url = uri("https://repo.clojars.org/")
    }
}




dependencies {
    implementation(project(":runtime"))
    implementation(project(":infrastructure"))
    implementation("org.clojure:clojure:1.8.0")
    implementation("jline:jline:2.14.2")
    implementation("reply:reply:0.3.8")
    implementation("com.offbytwo:docopt:0.6.0.20150202")
    implementation("org.fusesource.jansi:jansi:1.14")
    implementation("commons-fileupload:commons-fileupload:1.3.3")
    implementation("com.fasterxml.jackson.core:jackson-core:2.11.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile:2.11.0")
}

description = "cmdlets"

java {
    withJavadocJar()
}
