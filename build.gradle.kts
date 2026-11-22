plugins {
    base
}

subprojects {
    apply(plugin = "groovy")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(26))
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.14.1"))
        "testImplementation"("org.spockframework:spock-core:2.4-groovy-5.0")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
