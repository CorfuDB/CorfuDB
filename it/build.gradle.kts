
dependencies {
    implementation(project(":runtime"))
    implementation(project(":infrastructure"))
    implementation("com.spotify:docker-client:8.11.7")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.11.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-guava:2.11.0")
    implementation("org.apache.maven:maven-model:3.3.9")
    implementation("com.cloudbees.thirdparty:vijava:5.5-beta")
    implementation("org.apache.ant:ant-jsch:1.10.5")
}

description = "universe"

java {
    withJavadocJar()
}
