import net.ltgt.gradle.errorprone.errorprone
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    java
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
    alias(libs.plugins.spotless)
    alias(libs.plugins.errorprone) apply false
    jacoco
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    val myLibs = rootProject.extensions.getByType<org.gradle.accessors.dm.LibrariesForLibs>()

    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "net.ltgt.errorprone")
    apply(plugin = "jacoco")

    configure<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension> {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:3.3.4")
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    dependencies {
        implementation(myLibs.spring.boot.starter)
        implementation(myLibs.spring.boot.starter.actuator)
        implementation(myLibs.micrometer.registry.prometheus)

        "errorprone"(myLibs.errorprone.core)
        "errorprone"(myLibs.nullaway)

        testImplementation(myLibs.spring.boot.starter.test)
        testImplementation(myLibs.testcontainers.junit.jupiter)
        testImplementation(myLibs.awaitility)
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        finalizedBy(tasks.jacocoTestReport)
    }

    tasks.jacocoTestReport {
        dependsOn(tasks.test)
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    tasks.jacocoTestCoverageVerification {
        violationRules {
            rule {
                limit {
                    minimum = 0.80.toBigDecimal() // TODO: Raise to 90% once domain logic expands
                }
            }
        }
    }

    tasks.check {
        dependsOn(tasks.jacocoTestCoverageVerification)
    }

    spotless {
        java {
            googleJavaFormat()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.errorprone.disableWarningsInGeneratedCode.set(true)
        options.errorprone.errorproneArgs.addAll(
            "-Xep:NullAway:ERROR",
            "-XepOpt:NullAway:AnnotatedPackages=com.example.kafka"
        )
    }
}
