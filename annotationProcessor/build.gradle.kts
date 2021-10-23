
dependencies {
    implementation(project(":annotations"))
    implementation("com.squareup:javapoet:1.7.0")
}

description = "annotation-processor"

java {
    withJavadocJar()
}
