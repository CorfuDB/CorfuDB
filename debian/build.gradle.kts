repositories {
    maven {
        //isAllowInsecureProtocol = true
        url = uri("https://repo.clojars.org/")
    }
}

dependencies {
    implementation(project(":infrastructure"))
    implementation(project(":cmdlets"))
    implementation(project(":corfudb-tools"))
    implementation(project(":test"))
}

description = "debian"

java {
    withJavadocJar()
}
