plugins {
    application
}

application {
    mainClass = "io.github.alchevrier.feed.app.LoopbackDemo"
}

dependencies {
    implementation(project(":feed-core"))
    implementation(project(":order-book"))
    implementation(project(":nasdaq-feed"))
}
