plugins {
    application
}

application {
    mainClass = "io.github.alchevrier.feed.app.LoopbackDemo"
}

tasks.register<JavaExec>("loopbackBaseline") {
    group = "verification"
    description = "Measure paced dual-leg UDP-to-snapshot latency by book layout."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "io.github.alchevrier.feed.app.LoopbackBaseline"
    javaLauncher = javaToolchains.launcherFor(java.toolchain)
}

dependencies {
    implementation(project(":feed-core"))
    implementation(project(":order-book"))
    implementation(project(":nasdaq-feed"))
}
