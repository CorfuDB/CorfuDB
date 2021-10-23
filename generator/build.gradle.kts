
dependencies {
    implementation(project(":runtime"))
    implementation("commons-cli:commons-cli:1.3.1")
    implementation("ch.qos.logback:logback-classic:1.2.3")
}

description = "generator"

java {
    withJavadocJar()
}
