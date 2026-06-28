plugins {
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":common"))
    
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    
    compileOnly("org.springframework.boot:spring-boot-starter-web")
    
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.bootJar {
    enabled = false
}

tasks.jar {
    enabled = true
}
