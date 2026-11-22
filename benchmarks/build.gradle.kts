plugins {
    id("me.champeau.jmh") version "0.7.4"
}

dependencies {
    jmh(project(":feed-core"))
    jmh(project(":order-book"))
    jmh(project(":nasdaq-feed"))
    jmh(project(":app"))
}
