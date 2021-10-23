
dependencies {
    implementation(project(":runtime"))
    implementation(project(":test"))
    implementation("com.offbytwo:docopt:0.6.0.20150202")
    implementation("commons-lang:commons-lang:2.6")
}

description = "samples"

java {
    withJavadocJar()
}
