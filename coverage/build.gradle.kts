
dependencies {
    implementation(project(":runtime"))
    implementation(project(":infrastructure"))
    implementation(project(":annotations"))
    testImplementation(project(":test"))
    testImplementation(project(":universe"))
}

description = "coverage"

java {
    withJavadocJar()
}
