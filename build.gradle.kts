val ktorVersion = "2.0.1"
val kotlinVersion = "1.6.21"
val logbackVersion = "1.2.11"

System.setProperty("gradlew", File(".").resolve("gradlew.bat").absolutePath)
project.ext.set("mainClass", System.getProperty("mainClass") ?: "com.trader.defichain.ApplicationKt")

plugins {
    application
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.serialization") version "1.6.21"
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "com.trader.defichain"
version = "0.0.1"

application {
    mainClass.set(project.ext.get("mainClass").toString())

    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
}

dependencies {
    implementation("org.postgresql:postgresql:42.4.1")

    implementation("io.ktor:ktor-server-core-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-server-netty-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("org.codehaus.janino:janino:3.1.7")

    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-java:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-client-auth:$ktorVersion")
    implementation("io.ktor:ktor-client-encoding:$ktorVersion")

    implementation("org.zeromq:jeromq:0.5.2")

    testImplementation("io.ktor:ktor-server-tests-jvm:$ktorVersion")
    testImplementation("io.ktor:ktor-client-mock:$ktorVersion")
    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
    }
}
